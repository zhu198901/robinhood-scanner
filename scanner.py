
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
CHUNK_SIZE = int(os.getenv("LOG_CHUNK_SIZE", "200"))
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "300000"))
MIN_VOLUME_24H_USD = float(os.getenv("MIN_VOLUME_24H_USD", "100000"))
MAX_PUSHES = int(os.getenv("MAX_CANDIDATES_PER_RUN", "12"))
TIMEOUT = 20

V3_FACTORIES = {
    "Uniswap V3": "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA",
    "SushiSwap V3": "0xE51960f1B45f1C9FB6D166E6a884F866fC70433B",
}

V3_TOPIC = Web3.keccak(
    text="PoolCreated(address,address,uint24,int24,address)"
).hex()

session = requests.Session()
session.headers.update({
    "User-Agent": "RobinhoodReadOnlyScanner/1.0"
})


def log(message):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), message, flush=True)


def rpc(method, params):
    response = session.post(
        RPC_URL,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    result = response.json()

    if result.get("error"):
        raise RuntimeError(str(result["error"]))

    return result.get("result")


def topic_address(topic):
    return Web3.to_checksum_address("0x" + topic[-40:])


def data_address(data, word_index):
    raw = data[2:] if data.startswith("0x") else data
    start = word_index * 64
    word = raw[start:start + 64]

    if len(word) != 64 or int(word, 16) == 0:
        return ""

    return Web3.to_checksum_address("0x" + word[-40:])


def load_state():
    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(state, dict):
            return state
    except (OSError, ValueError):
        pass

    return {"last_block": None, "seen": []}


def save_state(state):
    state["seen"] = list(dict.fromkeys(state.get("seen", [])))[-10000:]
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def get_logs(start_block, end_block, factory):
    return rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
            "address": Web3.to_checksum_address(factory),
            "topics": [V3_TOPIC],
        }],
    ) or []


def discover_pools(start_block, end_block):
    discovered = []

    for start in range(start_block, end_block + 1, CHUNK_SIZE):
        end = min(end_block, start + CHUNK_SIZE - 1)

        for dex_name, factory in V3_FACTORIES.items():
            try:
                logs = get_logs(start, end, factory)

                for item in logs:
                    topics = item.get("topics", [])
                    if len(topics) < 3:
                        continue

                    token0 = topic_address(topics[1])
                    token1 = topic_address(topics[2])

                    # PoolCreated data:
                    # word 0 = fee, word 1 = tick spacing,
                    # word 2 = pool address.
                    pool = data_address(item.get("data", "0x"), 2)

                    if not pool or token0.lower() == token1.lower():
                        continue

                    tx_hash = item.get("transactionHash", "")
                    log_index = int(item.get("logIndex", "0x0"), 16)

                    discovered.append({
                        "dex": dex_name,
                        "token0": token0,
                        "token1": token1,
                        "pool": pool,
                        "block": int(item.get("blockNumber", "0x0"), 16),
                        "tx": tx_hash,
                        "log_index": log_index,
                    })

            except Exception as exc:
                log(f"{dex_name} 区块 {start}-{end} 查询失败：{exc}")

    return discovered


def get_market_pairs(token_address):
    url = (
        "https://api.dexscreener.com/latest/dex/tokens/"
        + token_address
    )

    try:
        response = session.get(url, timeout=TIMEOUT)

        if response.status_code == 429:
            log("Dexscreener 请求过于频繁")
            return []

        response.raise_for_status()
        payload = response.json()
        pairs = []

        for pair in payload.get("pairs") or []:
            chain = str(pair.get("chainId", "")).lower()

            if "robinhood" not in chain:
                continue

            liquidity = float(
                (pair.get("liquidity") or {}).get("usd") or 0
            )
            volume = float(
                (pair.get("volume") or {}).get("h24") or 0
            )

            pairs.append({
                "pair": pair.get("pairAddress", ""),
                "dex": pair.get("dexId", "unknown"),
                "url": pair.get("url", ""),
                "base_address": (
                    pair.get("baseToken") or {}
                ).get("address", ""),
                "name": (pair.get("baseToken") or {}).get("name", ""),
                "symbol": (
                    pair.get("baseToken") or {}
                ).get("symbol", ""),
                "quote": (
                    pair.get("quoteToken") or {}
                ).get("symbol", ""),
                "price": pair.get("priceUsd", "?"),
                "liquidity": liquidity,
                "volume": volume,
            })

        return pairs

    except Exception as exc:
        log(f"行情查询失败 {token_address}: {exc}")
        return []


def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        log("没有配置 SERVER_CHAN_KEY，无法发送微信通知")
        return False

    try:
        response = session.post(
            f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send",
            data={"title": title[:100], "desp": body},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        result = response.json()

        success = (
            result.get("code") == 0
            or (result.get("data") or {}).get("errno") == 0
        )

        if not success:
            log(f"Server酱发送失败：{result}")

        return success

    except Exception as exc:
        log(f"微信推送失败：{exc}")
        return False


def main():
    if not RPC_URL:
        raise SystemExit("错误：请配置 GitHub Secret ALCHEMY_RPC_URL")

    try:
        chain_id = int(rpc("eth_chainId", []), 16)

        if chain_id != CHAIN_ID:
            raise SystemExit(
                f"链 ID 不匹配：期望 {CHAIN_ID}，实际 {chain_id}"
            )

        latest_block = int(rpc("eth_blockNumber", []), 16)

    except Exception as exc:
        raise SystemExit(f"RPC 连接失败：{exc}")

    log(f"Robinhood Chain 连接成功，最新区块：{latest_block}")

    state = load_state()
    last_block = state.get("last_block")
    seen = set(state.get("seen", []))

    if last_block is None:
        start_block = max(0, latest_block - INITIAL_LOOKBACK + 1)
    else:
        start_block = max(0, int(last_block) + 1)

    if start_block > latest_block:
        log("没有新的区块需要扫描")
        return

    log(f"扫描区块范围：{start_block} - {latest_block}")
    pools = discover_pools(start_block, latest_block)
    log(f"发现新池事件：{len(pools)}")

    candidates_sent = 0

    for pool in pools:
        event_id = f"{pool['tx']}:{pool['log_index']}"

        if event_id in seen:
            continue

        seen.add(event_id)

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
            title = f"新池候选：{pair['symbol'] or token[:10]}"

            body = (
                f"来源：{pool['dex']}\n"
                f"代币地址：{token}\n"
                f"交易池：{pool['pool']}\n"
                f"行情交易对：{pair['pair']}\n"
                f"名称：{pair['name']}\n"
                f"代号：{pair['symbol']}\n"
                f"价格 USD：{pair['price']}\n"
                f"流动性 USD：{pair['liquidity']:.2f}\n"
                f"24小时成交量 USD：{pair['volume']:.2f}\n"
                f"DEX：{pair['dex']}\n"
                f"行情链接：{pair['url']}\n"
                f"创建区块：{pool['block']}\n"
                f"交易哈希：{pool['tx']}\n\n"
                "候选提醒，不是买入指令。尚未完成合约安全、"
                "持币集中度、买卖税和流动性锁定检查。"
            )

            log(
                f"候选：{token}，"
                f"流动性 ${pair['liquidity']:.0f}，"
                f"24小时成交量 ${pair['volume']:.0f}"
            )

            if send_wechat(title, body):
                candidates_sent += 1

            if candidates_sent >= MAX_PUSHES:
                log("达到本次推送上限")
                break

        if candidates_sent >= MAX_PUSHES:
            break

    state["last_block"] = latest_block
    state["seen"] = list(seen)[-10000:]
    save_state(state)

    log(
        f"扫描完成：发现 {len(pools)} 个池事件，"
        f"本次成功推送 {candidates_sent} 条"
    )


if __name__ == "__main__":
    main()
