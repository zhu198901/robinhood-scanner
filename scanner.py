
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
LOOKBACK = 30
CHUNK_SIZE = 10
REQUEST_DELAY = 1.2
MIN_LIQUIDITY_USD = 300000
MIN_VOLUME_24H_USD = 100000
MAX_PUSHES = 12
TIMEOUT = 20

FACTORIES = {
    "Uniswap V3": "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA",
    "SushiSwap V3": "0xE51960f1B45f1C9FB6D166E6a884F866fC70433B",
}

POOL_TOPIC = Web3.to_hex(
    Web3.keccak(
        text="PoolCreated(address,address,uint24,int24,address)"
    )
)

session = requests.Session()
session.headers.update({"User-Agent": "RobinhoodReadOnlyScanner/1.4"})
last_rpc_request = 0.0


def log(message):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), message, flush=True)


def rpc(method, params):
    global last_rpc_request

    if not RPC_URL:
        raise RuntimeError("缺少 ALCHEMY_RPC_URL")

    for attempt in range(5):
        wait = REQUEST_DELAY - (time.monotonic() - last_rpc_request)
        if wait > 0:
            time.sleep(wait)

        try:
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
            last_rpc_request = time.monotonic()

            if response.status_code == 429:
                delay = min(5 * (attempt + 1), 25)
                log(f"RPC 限流，等待 {delay} 秒后重试")
                time.sleep(delay)
                continue

            response.raise_for_status()
            data = response.json()

            if data.get("error"):
                err = data["error"]
                message = err.get("message", str(err))

                if err.get("code") == -32005 or "rate limit" in message.lower():
                    delay = min(5 * (attempt + 1), 25)
                    log(f"RPC 限流：{message}；等待 {delay} 秒")
                    time.sleep(delay)
                    continue

                raise RuntimeError(
                    f"RPC {method} 错误 {err.get('code')}: {message}"
                )

            if "result" not in data:
                raise RuntimeError(f"RPC 缺少 result：{data}")

            return data["result"]

        except requests.RequestException as exc:
            if attempt == 4:
                raise RuntimeError(f"RPC 网络请求失败：{exc}") from exc

            delay = min(3 * (attempt + 1), 15)
            log(f"网络异常，等待 {delay} 秒后重试：{exc}")
            time.sleep(delay)

    raise RuntimeError(f"RPC {method} 多次限流，已停止本次扫描")


def topic_address(topic):
    if not topic.startswith("0x"):
        topic = "0x" + topic
    return Web3.to_checksum_address("0x" + topic[-40:])


def data_address(data, index):
    raw = data[2:] if data.startswith("0x") else data
    word = raw[index * 64:(index + 1) * 64]

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


def get_logs(start, end, factory):
    if end - start + 1 > CHUNK_SIZE:
        raise ValueError("单次查询超过 10 个区块")

    return rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "address": Web3.to_checksum_address(factory),
            "topics": [POOL_TOPIC],
        }],
    ) or []


def discover(start, end):
    events_found = []

    for block_start in range(start, end + 1, CHUNK_SIZE):
        block_end = min(end, block_start + CHUNK_SIZE - 1)
        log(f"正在查询区块 {block_start}-{block_end}")

        for dex_name, factory in FACTORIES.items():
            events = get_logs(block_start, block_end, factory)

            for event in events:
                topics = event.get("topics", [])
                if len(topics) < 4:
                    continue

                token0 = topic_address(topics[1])
                token1 = topic_address(topics[2])

                # PoolCreated 的 data：word 0 是 tickSpacing，
                # word 1 是 pool 地址。
                pool_address = data_address(event.get("data", "0x"), 1)

                if not pool_address or token0.lower() == token1.lower():
                    continue

                events_found.append({
                    "dex": dex_name,
                    "token0": token0,
                    "token1": token1,
                    "pool": pool_address,
                    "block": int(event.get("blockNumber", "0x0"), 16),
                    "tx": event.get("transactionHash", ""),
                    "log_index": int(event.get("logIndex", "0x0"), 16),
                })

    return events_found


def market_pairs(token):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"

    try:
        response = session.get(url, timeout=TIMEOUT)

        if response.status_code == 429:
            log("行情 API 限流，暂时跳过该代币")
            return []

        response.raise_for_status()
        data = response.json()
        result = []

        for pair in data.get("pairs") or []:
            if "robinhood" not in str(pair.get("chainId", "")).lower():
                continue

            liquidity = float(
                (pair.get("liquidity") or {}).get("usd") or 0
            )
            volume = float(
                (pair.get("volume") or {}).get("h24") or 0
            )

            result.append({
                "url": pair.get("url", ""),
                "name": (pair.get("baseToken") or {}).get("name", ""),
                "symbol": (pair.get("baseToken") or {}).get("symbol", ""),
                "price": pair.get("priceUsd", "?"),
                "liquidity": liquidity,
                "volume": volume,
            })

        return result

    except Exception as exc:
        log(f"行情查询失败 {token}：{exc}")
        return []


def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        log("未设置 SERVER_CHAN_KEY，未发送微信推送")
        return False

    try:
        response = session.post(
            f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send",
            data={"title": title[:100], "desp": body},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        result = response.json()

        ok = (
            result.get("code") == 0
            or (result.get("data") or {}).get("errno") == 0
        )

        log("微信推送成功" if ok else f"微信推送失败：{result}")
        return ok

    except Exception as exc:
        log(f"微信推送失败：{exc}")
        return False


def main():
    chain_id = int(rpc("eth_chainId", []), 16)
    if chain_id != CHAIN_ID:
        raise RuntimeError(f"链 ID 不匹配：{chain_id}")

    latest = int(rpc("eth_blockNumber", []), 16)
    state = load_state()
    previous = state.get("last_block")

    if previous is None:
        start = max(0, latest - LOOKBACK + 1)
    else:
        start = int(previous) + 1

    log(f"Robinhood Chain 连接成功，最新区块：{latest}")
    log(f"事件主题：{POOL_TOPIC}")

    if start > latest:
        log("没有新块需要扫描")
        return

    log(f"本次扫描范围：{start}-{latest}")
    events = discover(start, latest)
    log(f"发现新池事件：{len(events)}")

    seen = set(state.get("seen", []))
    pushed = 0

    for event in events:
        event_id = f"{event['tx']}:{event['log_index']}"
        if event_id in seen:
            continue

        seen.add(event_id)

        for token in (event["token0"], event["token1"]):
            pairs = market_pairs(token)
            candidates = [
                pair for pair in pairs
                if pair["liquidity"] >= MIN_LIQUIDITY_USD
                and pair["volume"] >= MIN_VOLUME_24H_USD
            ]

            if not candidates:
                continue

            candidates.sort(
                key=lambda pair: (pair["liquidity"], pair["volume"]),
                reverse=True,
            )
            pair = candidates[0]

            title = f"新池候选：{pair['symbol'] or token[:10]}"
            body = (
                f"DEX：{event['dex']}\n"
                f"代币地址：{token}\n"
                f"池地址：{event['pool']}\n"
                f"名称：{pair['name']}\n"
                f"代号：{pair['symbol']}\n"
                f"价格 USD：{pair['price']}\n"
                f"流动性 USD：{pair['liquidity']:.2f}\n"
                f"24 小时成交量 USD：{pair['volume']:.2f}\n"
                f"行情链接：{pair['url']}\n"
                f"区块：{event['block']}\n"
                f"交易：{event['tx']}\n\n"
                "这是候选提醒，不代表安全或建议买入。"
                "尚未验证合约税费、貔貅风险、持币集中度和 LP 锁定。"
                "本程序只读，不会自动买卖。"
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
    log(f"扫描结束：新池事件 {len(events)}，成功推送 {pushed} 条")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"程序停止：{exc}")
        raise SystemExit(1)
