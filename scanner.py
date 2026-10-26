
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
INITIAL_LOOKBACK = max(10, int(os.getenv("INITIAL_LOOKBACK_BLOCKS", "120")))
CHUNK_SIZE = 10
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "300000"))
MIN_VOLUME_24H_USD = float(os.getenv("MIN_VOLUME_24H_USD", "100000"))
MAX_PUSHES = max(1, int(os.getenv("MAX_CANDIDATES_PER_RUN", "12")))
TIMEOUT = 20

FACTORIES = {
    "Uniswap V3": "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA",
    "SushiSwap V3": "0xE51960f1B45f1C9FB6D166E6a884F866fC70433B",
}

# Explicitly use Web3.to_hex to guarantee a 0x-prefixed topic.
POOL_TOPIC = Web3.to_hex(
    Web3.keccak(text="PoolCreated(address,address,uint24,int24,address)")
)

session = requests.Session()
session.headers.update({"User-Agent": "RobinhoodReadOnlyScanner/1.3"})


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
    data = response.json()

    if data.get("error"):
        error = data["error"]
        raise RuntimeError(
            f"RPC {method} code={error.get('code')}: "
            f"{error.get('message', str(error))}"
        )

    if "result" not in data:
        raise RuntimeError(f"RPC 响应缺少 result：{str(data)[:500]}")

    return data["result"]


def topic_address(topic):
    value = topic if topic.startswith("0x") else "0x" + topic
    return Web3.to_checksum_address("0x" + value[-40:])


def data_address(data, word_index):
    raw = data[2:] if data.startswith("0x") else data
    word = raw[word_index * 64:(word_index + 1) * 64]

    if len(word) != 64 or int(word, 16) == 0:
        return ""

    return Web3.to_checksum_address("0x" + word[-40:])


def load_state():
    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(state, dict):
            state.setdefault("last_block", None)
            state.setdefault("seen", [])
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


def get_logs(start, end, factory):
    if start < 0 or end < start:
        raise ValueError(f"区块范围不合法：{start}-{end}")

    if end - start + 1 > CHUNK_SIZE:
        raise ValueError(
            f"单次查询超过 {CHUNK_SIZE} 个区块：{start}-{end}"
        )

    params = [{
        "fromBlock": hex(start),
        "toBlock": hex(end),
        "address": Web3.to_checksum_address(factory),
        "topics": [POOL_TOPIC],
    }]

    return rpc("eth_getLogs", params) or []


def discover(start, end):
    found = []

    for block_start in range(start, end + 1, CHUNK_SIZE):
        block_end = min(end, block_start + CHUNK_SIZE - 1)

        log(f"正在查询区块 {block_start}-{block_end}")

        for dex_name, factory in FACTORIES.items():
            try:
                events = get_logs(block_start, block_end, factory)

                for event in events:
                    topics = event.get("topics", [])
                    if len(topics) < 4:
                        continue

                    token0 = topic_address(topics[1])
                    token1 = topic_address(topics[2])

                    # PoolCreated:
                    # topics[3] = indexed fee
                    # data word 0 = tickSpacing
                    # data word 1 = pool address
                    pool_address = data_address(
                        event.get("data", "0x"), 1
                    )

                    if not pool_address:
                        continue

                    if token0.lower() == token1.lower():
                        continue

                    found.append({
                        "dex": dex_name,
                        "token0": token0,
                        "token1": token1,
                        "pool": pool_address,
                        "block": int(event["blockNumber"], 16),
                        "tx": event.get("transactionHash", ""),
                        "log_index": int(event.get("logIndex", "0x0"), 16),
                    })

            except Exception as exc:
                raise RuntimeError(
                    f"{dex_name} 查询区块 {block_start}-{block_end} 失败："
                    f"{exc}"
                ) from exc

    return found


def market_pairs(token):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"

    try:
        response = session.get(url, timeout=TIMEOUT)

        if response.status_code == 429:
            log("Dexscreener 请求过于频繁，暂时跳过该代币")
            return []

        response.raise_for_status()
        data = response.json()
        pairs = []

        for pair in data.get("pairs") or []:
            chain = str(pair.get("chainId", "")).lower()

            # Only accept pairs identified by the API as Robinhood Chain.
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
                "name": (pair.get("baseToken") or {}).get("name", ""),
                "symbol": (pair.get("baseToken") or {}).get("symbol", ""),
                "price": pair.get("priceUsd", "?"),
                "liquidity": liquidity,
                "volume": volume,
            })

        return pairs

    except Exception as exc:
        log(f"行情查询失败 {token}：{exc}")
        return []


def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        log("未配置 SERVER_CHAN_KEY，跳过微信推送")
        return False

    url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"

    try:
        response = session.post(
            url,
            data={"title": title[:100], "desp": body},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        result = response.json()

        success = (
            result.get("code") == 0
            or (result.get("data") or {}).get("errno") == 0
        )

        if success:
            log("微信推送成功")
        else:
            log(f"微信推送失败：{result}")

        return success

    except Exception as exc:
        log(f"微信推送异常：{exc}")
        return False


def main():
    if not RPC_URL:
        raise RuntimeError("缺少 GitHub Secret：ALCHEMY_RPC_URL")

    chain_result = rpc("eth_chainId", [])
    chain_id = int(chain_result, 16)

    if chain_id != CHAIN_ID:
        raise RuntimeError(
            f"链 ID 不匹配：期望 {CHAIN_ID}，实际 {chain_id}"
        )

    latest = int(rpc("eth_blockNumber", []), 16)
    state = load_state()
    previous = state.get("last_block")

    if previous is None:
        start = max(0, latest - INITIAL_LOOKBACK + 1)
    else:
        start = int(previous) + 1

    log(f"Robinhood Chain 连接成功，最新区块：{latest}")
    log(f"事件主题：{POOL_TOPIC}")

    if start > latest:
        log("没有新区块需要扫描")
        return

    log(f"扫描区块范围：{start}-{latest}")
    pools = discover(start, latest)
    log(f"发现新池事件：{len(pools)}")

    seen = set(state.get("seen", []))
    pushed = 0

    for pool in pools:
        event_id = f"{pool['tx']}:{pool['log_index']}"

        if event_id in seen:
            continue

        seen.add(event_id)

        for token in (pool["token0"], pool["token1"]):
            pairs = market_pairs(token)

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
                f"池地址：{pool['pool']}\n"
                f"名称：{pair['name']}\n"
                f"代号：{pair['symbol']}\n"
                f"价格 USD：{pair['price']}\n"
                f"流动性 USD：{pair['liquidity']:.2f}\n"
                f"24小时成交量 USD：{pair['volume']:.2f}\n"
                f"行情链接：{pair['url']}\n"
                f"创建区块：{pool['block']}\n"
                f"交易哈希：{pool['tx']}\n\n"
                "注意：这只是候选提醒，不代表安全或值得买入。"
                "尚未验证合约税费、貔貅风险、持币集中度及 LP 锁定。"
                "本程序只读，不会自动买入或卖出。"
            )

            if send_wechat(title, body):
                pushed += 1

            if pushed >= MAX_PUSHES:
                break

        if pushed >= MAX_PUSHES:
            break

    state["last_block"] = latest
    state["seen"] = list(seen)[-10000:]
    save_state(state)

    log(
        f"扫描完成：新池事件 {len(pools)}，"
        f"成功推送 {pushed} 条"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"程序停止：{exc}")
        raise SystemExit(1)
