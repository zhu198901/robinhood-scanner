
# Robinhood Chain read-only scanner
# No wallet, no private key, no automatic trading.

import json
import os
import time
from pathlib import Path

import requests
from web3 import Web3

RPC_URL = os.getenv("ALCHEMY_RPC_URL", "").strip()
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "").strip()

CHAIN_ID = 4663
STATE_FILE = Path("state.json")

INITIAL_LOOKBACK = int(os.getenv("INITIAL_LOOKBACK_BLOCKS", "1200"))
CHUNK_SIZE = int(os.getenv("LOG_CHUNK_SIZE", "10"))
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "300000"))
MIN_VOLUME_24H_USD = float(os.getenv("MIN_VOLUME_24H_USD", "100000"))
MAX_PUSHES = int(os.getenv("MAX_CANDIDATES_PER_RUN", "12"))
TIMEOUT = 25
MAX_RETRIES = 3

FACTORIES = {
    "Uniswap V3": "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA",
    "SushiSwap V3": "0xE51960f1B45f1C9FB6D166E6a884F866fC70433B",
}

POOL_CREATED_TOPIC = Web3.keccak(
    text="PoolCreated(address,address,uint24,int24,address)"
).hex()

session = requests.Session()
session.headers.update({
    "User-Agent": "RobinhoodReadOnlyScanner/1.1"
})


def log(message):
    print(
        time.strftime("%Y-%m-%d %H:%M:%S"),
        message,
        flush=True,
    )


def rpc(method, params):
    if not RPC_URL:
        raise RuntimeError("ALCHEMY_RPC_URL 未配置")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(
                RPC_URL,
                json=payload,
                timeout=TIMEOUT,
            )

            if not response.ok:
                detail = response.text[:1200]
                raise RuntimeError(
                    f"HTTP {response.status_code}: {detail}"
                )

            try:
                data = response.json()
            except ValueError:
                raise RuntimeError(
                    f"RPC 返回的不是 JSON：{response.text[:500]}"
                )

            if data.get("error"):
                error = data["error"]
                raise RuntimeError(
                    f"RPC 错误 code={error.get('code')}: "
                    f"{error.get('message', str(error))}"
                )

            if "result" not in data:
                raise RuntimeError(
                    f"RPC 响应缺少 result：{str(data)[:500]}"
                )

            return data["result"]

        except Exception as exc:
            last_error = exc
            log(
                f"{method} 请求失败 "
                f"({attempt}/{MAX_RETRIES})：{exc}"
            )

            if attempt < MAX_RETRIES:
                time.sleep(attempt * 2)

    raise RuntimeError(f"{method} 最终失败：{last_error}")


def topic_address(topic):
    return Web3.to_checksum_address("0x" + topic[-40:])


def data_address(data, word_index):
    raw = data[2:] if data.startswith("0x") else data
    start = word_index * 64
    word = raw[start:start + 64]

    if len(word) != 64:
        return ""

    if int(word, 16) == 0:
        return ""

    return Web3.to_checksum_address("0x" + word[-40:])


def load_state():
    try:
        data = json.loads(
            STATE_FILE.read_text(encoding="utf-8")
        )

        if isinstance(data, dict):
            return data

    except (OSError, ValueError):
        pass

    return {
        "last_block": None,
        "seen": [],
    }


def save_state(state):
    state["seen"] = list(
        dict.fromkeys(state.get("seen", []))
    )[-10000:]

    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def fetch_logs(start_block, end_block, factory):
    params = [{
        "fromBlock": hex(start_block),
        "toBlock": hex(end_block),
        "address": Web3.to_checksum_address(factory),
        "topics": [POOL_CREATED_TOPIC],
    }]

    result = rpc("eth_getLogs", params)

    if not isinstance(result, list):
        raise RuntimeError(
            f"eth_getLogs 返回格式异常：{str(result)[:500]}"
        )

    return result


def discover_pools(start_block, end_block):
    pools = []
    failed_ranges = []

    for start in range(
        start_block,
        end_block + 1,
        CHUNK_SIZE,
    ):
        end = min(end_block, start + CHUNK_SIZE - 1)

        for dex_name, factory in FACTORIES.items():
            try:
                events = fetch_logs(start, end, factory)

                for event in events:
                    topics = event.get("topics", [])

                    if len(topics) < 3:
                        continue

                    token0 = topic_address(topics[1])
                    token1 = topic_address(topics[2])

                    # Uniswap V3 PoolCreated data:
                    # word 0: fee
                    # word 1: tickSpacing
                    # word 2: pool address
                    pool_address = data_address(
                        event.get("data", "0x"),
                        2,
                    )

                    if not pool_address:
                        log(
                            f"{dex_name} 事件缺少池地址，"
                            f"交易：{event.get('transactionHash', '')}"
                        )
                        continue

                    if token0.lower() == token1.lower():
                        continue

                    pools.append({
                        "dex": dex_name,
                        "token0": token0,
                        "token1": token1,
                        "pool": pool_address,
                        "block": int(
                            event.get("blockNumber", "0x0"),
                            16,
                        ),
                        "tx": event.get("transactionHash", ""),
                        "log_index": int(
                            event.get("logIndex", "0x0"),
                            16,
                        ),
                    })

            except Exception as exc:
                failed_ranges.append((start, end, dex_name))
                log(
                    f"{dex_name} 区块 {start}-{end} "
                    f"查询失败：{exc}"
                )

    return pools, failed_ranges


def get_market_pairs(token_address):
    url = (
        "https://api.dexscreener.com/latest/dex/tokens/"
        + token_address
    )

    try:
        response = session.get(url, timeout=TIMEOUT)

        if response.status_code == 429:
            log("Dexscreener 限流，本次跳过该代币")
            return []

        if not response.ok:
            log(
                f"Dexscreener HTTP {response.status_code}："
                f"{response.text[:300]}"
            )
            return []

        payload = response.json()
        pairs = payload.get("pairs") or []

        results = []

        for pair in pairs:
            chain_id = str(pair.get("chainId", "")).lower()

            if "robinhood" not in chain_id:
                continue

            liquidity = float(
                (pair.get("liquidity") or {}).get("usd") or 0
            )
            volume = float(
                (pair.get("volume") or {}).get("h24") or 0
            )

            results.append({
                "pair": pair.get("pairAddress", ""),
                "dex": pair.get("dexId", "unknown"),
                "url": pair.get("url", ""),
                "name": (
                    pair.get("baseToken") or {}
                ).get("name", ""),
                "symbol": (
                    pair.get("baseToken") or {}
                ).get("symbol", ""),
                "price": pair.get("priceUsd", "?"),
                "liquidity": liquidity,
                "volume": volume,
            })

        return results

    except Exception as exc:
        log(f"行情查询失败 {token_address}：{exc}")
        return []


def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        log("SERVER_CHAN_KEY 未配置，未发送微信通知")
        return False

    url = (
        "https://sctapi.ftqq.com/"
        + SERVER_CHAN_KEY
        + ".send"
    )

    try:
        response = session.post(
            url,
            data={
                "title": title[:100],
                "desp": body,
            },
            timeout=TIMEOUT,
        )

        if not response.ok:
            log(
                f"Server酱 HTTP {response.status_code}："
                f"{response.text[:500]}"
            )
            return False

        result = response.json()

        success = (
            result.get("code") == 0
            or (result.get("data") or {}).get("errno") == 0
        )

        if success:
            log(f"微信通知发送成功：{title}")
        else:
            log(f"Server酱发送失败：{result}")

        return success

    except Exception as exc:
        log(f"微信通知异常：{exc}")
        return False


def main():
    if not RPC_URL:
        raise SystemExit(
            "缺少 GitHub Secret：ALCHEMY_RPC_URL"
        )

    try:
        chain_result = rpc("eth_chainId", [])
        chain_id = int(chain_result, 16)

        if chain_id != CHAIN_ID:
            raise SystemExit(
                f"链 ID 不匹配：期望 {CHAIN_ID}，"
                f"实际 {chain_id}"
            )

        latest_block = int(
            rpc("eth_blockNumber", []),
            16,
        )

    except Exception as exc:
        raise SystemExit(f"RPC 初始化失败：{exc}")

    log(
        f"Robinhood Chain 连接成功，"
        f"最新区块：{latest_block}"
    )

    state = load_state()
    last_block = state.get("last_block")
    seen = set(state.get("seen", []))

    if last_block is None:
        start_block = max(
            0,
            latest_block - INITIAL_LOOKBACK + 1,
        )
    else:
        start_block = max(0, int(last_block) + 1)

    if start_block > latest_block:
        log("没有新的区块需要扫描")
        return

    log(
        f"扫描区块范围："
        f"{start_block} - {latest_block}"
    )

    pools, failed_ranges = discover_pools(
        start_block,
        latest_block,
    )

    log(f"发现新池事件：{len(pools)}")
    log(f"失败的区块查询次数：{len(failed_ranges)}")

    # If any range failed, do not advance the saved block.
    # The next run can retry instead of silently skipping blocks.
    if failed_ranges:
        log(
            "本轮有区块查询失败。为防止漏扫，"
            "不更新 last_block；请先检查 RPC 错误。"
        )
        raise SystemExit(
            "扫描不完整：存在失败的日志查询，"
            "已保留旧进度，请修复后重试。"
        )

    candidates_sent = 0
    new_seen = set(seen)

    for pool in pools:
        event_id = (
            f"{pool['tx']}:{pool['log_index']}"
        )

        if event_id in new_seen:
            continue

        new_seen.add(event_id)

        for token in (pool["token0"], pool["token1"]):
            pairs = get_market_pairs(token)

            eligible = [
                pair for pair in pairs
                if pair["liquidity"] >= MIN_LIQUIDITY_USD
                and pair["volume"] >= MIN_VOLUME_24H_USD
            ]

            if not eligible:
                continue

            eligible.sort(
                key=lambda pair: (
                    pair["liquidity"],
                    pair["volume"],
                ),
                reverse=True,
            )

            pair = eligible[0]

            title = (
                "新池候选："
                + (pair["symbol"] or token[:10])
            )

            body = (
                f"来源：{pool['dex']}\n"
                f"代币地址：{token}\n"
                f"池地址：{pool['pool']}\n"
                f"行情交易对：{pair['pair']}\n"
                f"代币名称：{pair['name']}\n"
                f"代号：{pair['symbol']}\n"
                f"价格 USD：{pair['price']}\n"
                f"流动性 USD：{pair['liquidity']:.2f}\n"
                f"24小时成交量 USD：{pair['volume']:.2f}\n"
                f"DEX：{pair['dex']}\n"
                f"行情链接：{pair['url']}\n"
                f"创建区块：{pool['block']}\n"
                f"交易哈希：{pool['tx']}\n\n"
                "注意：仅为候选提醒，不是买入建议。"
                "尚未完成合约安全、持币集中度、"
                "买卖税及流动性锁定检查。"
            )

            log(
                f"发现候选 {token}；"
                f"流动性=${pair['liquidity']:.0f}；"
                f"24小时成交量=${pair['volume']:.0f}"
            )

            if send_wechat(title, body):
                candidates_sent += 1

            if candidates_sent >= MAX_PUSHES:
                break

        if candidates_sent >= MAX_PUSHES:
            break

    state["last_block"] = latest_block
    state["seen"] = list(new_seen)[-10000:]
    save_state(state)

    log(
        f"扫描完成：发现 {len(pools)} 个池事件，"
        f"成功推送 {candidates_sent} 条"
    )


if __name__ == "__main__":
    main()
