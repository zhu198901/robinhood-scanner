
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
INITIAL_LOOKBACK = int(os.getenv("INITIAL_LOOKBACK_BLOCKS", "120"))
CHUNK_SIZE = 10
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "300000"))
MIN_VOLUME_24H_USD = float(os.getenv("MIN_VOLUME_24H_USD", "100000"))
MAX_PUSHES = int(os.getenv("MAX_CANDIDATES_PER_RUN", "12"))
TIMEOUT = 20

FACTORIES = {
    "Uniswap V3": "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA",
    "SushiSwap V3": "0xE51960f1B45f1C9FB6D166E6a884F866fC70433B",
}

POOL_TOPIC = Web3.keccak(
    text="PoolCreated(address,address,uint24,int24,address)"
).hex()

session = requests.Session()
session.headers.update({"User-Agent": "RobinhoodReadOnlyScanner/1.2"})


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

    if not response.ok:
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.text[:1000]}"
        )

    data = response.json()

    if data.get("error"):
        error = data["error"]
        raise RuntimeError(
            f"RPC code={error.get('code')}: "
            f"{error.get('message', str(error))}"
        )

    if "result" not in data:
        raise RuntimeError(f"RPC 响应格式异常：{str(data)[:500]}")

    return data["result"]


def topic_address(topic):
    return Web3.to_checksum_address("0x" + topic[-40:])


def data_address(data, index):
    raw = data[2:] if data.startswith("0x") else data
    word = raw[index * 64:(index + 1) * 64]

    if len(word) != 64 or int(word, 16) == 0:
        return ""

    return Web3.to_checksum_address("0x" + word[-40:])


def load_state():
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
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
    # JSON-RPC block quantities must be 0x-prefixed hex strings.
    params = [{
        "fromBlock": hex(start),
        "toBlock": hex(end),
        "address": Web3.to_checksum_address(factory),
        "topics": [POOL_TOPIC],
    }]

    return rpc("eth_getLogs", params)


def discover(start, end):
    pools = []

    for block_start in range(start, end + 1, CHUNK_SIZE):
        block_end = min(end, block_start + CHUNK_SIZE - 1)

        for dex, factory in FACTORIES.items():
            try:
                events = get_logs(block_start, block_end, factory)

                for event in events:
                    topics = event.get("topics", [])
                    if len(topics) < 3:
                        continue

                    token0 = topic_address(topics[1])
                    token1 = topic_address(topics[2])

                    # PoolCreated data words:
                    # 0 = fee, 1 = tick spacing, 2 = pool address.
                    pool_address = data_address(
                        event.get("data", "0x"), 2
                    )

                    if not pool_address or token0.lower() == token1.lower():
                        continue

                    pools.append({
                        "dex": dex,
                        "token0": token0,
                        "token1": token1,
                        "pool": pool_address,
                        "block": int(event.get("blockNumber", "0x0"), 16),
                        "tx": event.get("transactionHash", ""),
                        "log_index": int(event.get("logIndex", "0x0"), 16),
                    })

            except Exception as exc:
                # Stop immediately: do not spend hours retrying bad requests.
                raise RuntimeError(
                    f"{dex} 区块 {block_start}-{block_end} 查询失败：{exc}"
                ) from exc

    return pools


def market_pairs(token):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"

    try:
        response = session.get(url, timeout=TIMEOUT)
        if not response.ok:
            log(f"行情接口 HTTP {response.status_code}：{response.text[:200]}")
            return []

        payload = response.json()
        results = []

        for pair in payload.get("pairs") or []:
            if "robinhood" not in str(pair.get("chainId", "")).lower():
                continue

            liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
            volume = float((pair.get("volume") or {}).get("h24") or 0)

            results.append({
                "pair": pair.get("pairAddress", ""),
                "dex": pair.get("dexId", "unknown"),
                "url": pair.get("url", ""),
                "name": (pair.get("baseToken") or {}).get("name", ""),
                "symbol": (pair.get("baseToken") or {}).get("symbol", ""),
                "price": pair.get("priceUsd", "?"),
                "liquidity": liquidity,
                "volume": volume,
            })

        return results

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

        if not response.ok:
            log(f"Server酱 HTTP {response.status_code}：{response.text[:300]}")
            return False

        result = response.json()
        success = (
            result.get("code") == 0
            or (result.get("data") or {}).get("errno") == 0
        )

        log("微信推送成功" if success else f"微信推送失败：{result}")
        return success

    except Exception as exc:
        log(f"微信推送异常：{exc}")
        return False


def main():
    if not RPC_URL:
        raise SystemExit("缺少 GitHub Secret ALCHEMY_RPC_URL")

    chain_id = int(rpc("eth_chainId", []), 16)
    if chain_id != CHAIN_ID:
        raise SystemExit(f"链 ID 不匹配：{chain_id}")

    latest = int(rpc("eth_blockNumber", []), 16)
    state = load_state()
    last_block = state.get("last_block")

    if last_block is None:
        start = max(0, latest - INITIAL_LOOKBACK + 1)
    else:
        start = int(last_block) + 1

    log(f"Robinhood Chain 连接成功，最新区块：{latest}")

    if start > latest:
        log("没有新块需要扫描")
        return

    log(f"扫描区块范围：{start} - {latest}")
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
            eligible = [
                p for p in market_pairs(token)
                if p["liquidity"] >= MIN_LIQUIDITY_USD
                and p["volume"] >= MIN_VOLUME_24H_USD
            ]

            if not eligible:
                continue

            eligible.sort(
                key=lambda p: (p["liquidity"], p["volume"]),
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
                f"区块：{pool['block']}\n"
                f"交易：{pool['tx']}\n\n"
                "仅为候选提醒，尚未完成合约安全、税费、"
                "持币集中度和流动性锁定检查；不自动买卖。"
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

    log(f"扫描完成：新池事件 {len(pools)}，成功推送 {pushed} 条")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"程序停止：{exc}")
        raise SystemExit(1)
