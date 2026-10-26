
import json
import os
import time
from pathlib import Path

import requests
from web3 import Web3

# ==================== 基本配置 ====================

RPC_URL = os.getenv("ALCHEMY_RPC_URL", "").strip()
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "").strip()

CHAIN_ID = 4663
STATE_FILE = Path("state.json")

# 首次运行向前扫描多少个区块
INITIAL_LOOKBACK_BLOCKS = 30

# 每次运行最多处理多少个新区块，防止任务运行过久
MAX_BLOCKS_PER_RUN = 300

# 单次 eth_getLogs 查询的区块数量
CHUNK_SIZE = 10

# RPC 请求间隔，降低限流概率
REQUEST_DELAY = 0.35
TIMEOUT = 20
RPC_RETRIES = 5

# 候选币筛选门槛
MIN_LIQUIDITY_USD = 300000
MIN_VOLUME_24H_USD = 100000
MAX_PUSHES = 12

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
session.headers.update({
    "User-Agent": "RobinhoodReadOnlyScanner/2.0"
})

last_rpc_request = 0.0


# ==================== 日志与 RPC ====================

def log(message):
    print(
        time.strftime("%Y-%m-%d %H:%M:%S"),
        message,
        flush=True,
    )


def rpc(method, params):
    global last_rpc_request

    if not RPC_URL:
        raise RuntimeError(
            "缺少 ALCHEMY_RPC_URL，请检查 GitHub Secrets"
        )

    for attempt in range(RPC_RETRIES):
        wait = REQUEST_DELAY - (
            time.monotonic() - last_rpc_request
        )

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
                delay = min(2 ** (attempt + 1), 20)
                log(f"RPC 限流，{delay} 秒后重试")
                time.sleep(delay)
                continue

            response.raise_for_status()
            data = response.json()

            if data.get("error"):
                err = data["error"]
                message = str(err.get("message", err))
                lower = message.lower()

                retryable = (
                    err.get("code") == -32005
                    or "rate limit" in lower
                    or "too many requests" in lower
                    or "temporarily unavailable" in lower
                    or "timeout" in lower
                )

                if retryable:
                    delay = min(2 ** (attempt + 1), 20)
                    log(f"RPC 暂时不可用，等待 {delay} 秒")
                    time.sleep(delay)
                    continue

                raise RuntimeError(
                    f"RPC {method} 错误 "
                    f"{err.get('code')}: {message}"
                )

            if "result" not in data:
                raise RuntimeError(
                    f"RPC 响应缺少 result：{data}"
                )

            return data["result"]

        except requests.RequestException as exc:
            delay = min(2 ** (attempt + 1), 20)
            log(
                f"网络请求失败，第 {attempt + 1} 次尝试：{exc}"
            )

            if attempt + 1 < RPC_RETRIES:
                time.sleep(delay)
            else:
                raise RuntimeError(
                    f"RPC 网络请求多次失败：{exc}"
                ) from exc

    raise RuntimeError(
        f"RPC {method} 多次重试失败，本次任务停止"
    )


# ==================== 数据处理 ====================

def topic_address(topic):
    if not isinstance(topic, str):
        return ""

    if not topic.startswith("0x"):
        topic = "0x" + topic

    if len(topic) < 42:
        return ""

    try:
        return Web3.to_checksum_address(
            "0x" + topic[-40:]
        )
    except ValueError:
        return ""


def data_address(data, index):
    if not isinstance(data, str):
        return ""

    raw = data[2:] if data.startswith("0x") else data
    start = index * 64
    word = raw[start:start + 64]

    if len(word) != 64:
        return ""

    try:
        if int(word, 16) == 0:
            return ""

        return Web3.to_checksum_address(
            "0x" + word[-40:]
        )
    except ValueError:
        return ""


def load_state():
    try:
        state = json.loads(
            STATE_FILE.read_text(encoding="utf-8")
        )

        if isinstance(state, dict):
            if not isinstance(state.get("seen"), list):
                state["seen"] = []
            return state

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

    temporary = STATE_FILE.with_suffix(".tmp")

    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    temporary.replace(STATE_FILE)


# ==================== 新池事件扫描 ====================

def get_logs(start, end, factory):
    if start < 0 or end < start:
        raise ValueError("区块范围无效")

    if end - start + 1 > CHUNK_SIZE:
        raise ValueError("单次查询区块数超过限制")

    return rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "address": Web3.to_checksum_address(factory),
            "topics": [POOL_TOPIC],
        }],
    ) or []


def get_logs_resilient(start, end, factory, dex_name):
    """
    如果 RPC 不接受当前区块范围，逐步缩小查询范围。
    只有查询成功才返回，不会把失败误当成没有新池。
    """
    try:
        return get_logs(start, end, factory)

    except Exception as exc:
        message = str(exc).lower()

        range_error = (
            "block range" in message
            or "too many blocks" in message
            or "query returned more than" in message
            or "eth_getlogs" in message and "range" in message
        )

        if start < end and range_error:
            middle = (start + end) // 2
            log(
                f"{dex_name} 查询范围过大，拆分为更小区间"
            )

            first = get_logs_resilient(
                start, middle, factory, dex_name
            )
            second = get_logs_resilient(
                middle + 1, end, factory, dex_name
            )

            return first + second

        raise


def discover(start, end):
    events_found = []

    for block_start in range(
        start, end + 1, CHUNK_SIZE
    ):
        block_end = min(
            end, block_start + CHUNK_SIZE - 1
        )

        log(
            f"正在查询区块 {block_start}-{block_end}"
        )

        for dex_name, factory in FACTORIES.items():
            events = get_logs_resilient(
                block_start,
                block_end,
                factory,
                dex_name,
            )

            for event in events:
                topics = event.get("topics", [])

                if len(topics) < 4:
                    continue

                token0 = topic_address(topics[1])
                token1 = topic_address(topics[2])

                # PoolCreated 的 data：
                # word 0 = tickSpacing
                # word 1 = pool 地址
                pool_address = data_address(
                    event.get("data", "0x"), 1
                )

                if not token0 or not token1 or not pool_address:
                    continue

                if token0.lower() == token1.lower():
                    continue

                events_found.append({
                    "dex": dex_name,
                    "token0": token0,
                    "token1": token1,
                    "pool": pool_address,
                    "block": int(
                        event.get("blockNumber", "0x0"), 16
                    ),
                    "tx": event.get("transactionHash", ""),
                    "log_index": int(
                        event.get("logIndex", "0x0"), 16
                    ),
                })

    # 去除重复事件
    unique = {}

    for event in events_found:
        key = (
            event["tx"],
            event["log_index"],
        )
        unique[key] = event

    return list(unique.values())


# ==================== 行情筛选 ====================

def market_pairs(token):
    url = (
        "https://api.dexscreener.com/latest/dex/tokens/"
        + token
    )

    try:
        response = session.get(url, timeout=TIMEOUT)

        if response.status_code == 429:
            log("DexScreener 限流，暂时跳过该代币")
            return []

        response.raise_for_status()
        data = response.json()
        result = []

        for pair in data.get("pairs") or []:
            chain = str(
                pair.get("chainId", "")
            ).lower()

            # 只保留链名称包含 robinhood 的结果。
            # 如果 DexScreener 使用其他链标识，
            # 该标识需要后续核实后再加入。
            if "robinhood" not in chain:
                continue

            try:
                liquidity = float(
                    (pair.get("liquidity") or {}).get("usd") or 0
                )
                volume = float(
                    (pair.get("volume") or {}).get("h24") or 0
                )
            except (TypeError, ValueError):
                continue

            base = pair.get("baseToken") or {}
            quote = pair.get("quoteToken") or {}

            result.append({
                "url": pair.get("url", ""),
                "pair_address": pair.get("pairAddress", ""),
                "base_address": base.get("address", ""),
                "name": base.get("name", ""),
                "symbol": base.get("symbol", ""),
                "quote": quote.get("symbol", ""),
                "price": pair.get("priceUsd", "?"),
                "liquidity": liquidity,
                "volume": volume,
            })

        return result

    except Exception as exc:
        log(f"行情查询失败 {token}：{exc}")
        return []


# ==================== 微信推送 ====================

def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        log("未设置 SERVER_CHAN_KEY，无法推送微信")
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

        response.raise_for_status()
        result = response.json()

        ok = (
            result.get("code") == 0
            or (result.get("data") or {}).get("errno") == 0
        )

        if ok:
            log(f"微信推送成功：{title}")
        else:
            log(f"微信推送失败：{result}")

        return ok

    except Exception as exc:
        log(f"微信推送请求失败：{exc}")
        return False


# ==================== 主程序 ====================

def main():
    chain_id = int(
        rpc("eth_chainId", []), 16
    )

    if chain_id != CHAIN_ID:
        raise RuntimeError(
            f"链 ID 不匹配：预期 {CHAIN_ID}，实际 {chain_id}"
        )

    latest = int(
        rpc("eth_blockNumber", []), 16
    )

    state = load_state()
    previous = state.get("last_block")

    if previous is None:
        start = max(
            0,
            latest - INITIAL_LOOKBACK_BLOCKS + 1,
        )
    else:
        start = int(previous) + 1

    log(
        f"Robinhood Chain 连接成功，"
        f"最新区块：{latest}"
    )

    log(f"PoolCreated 事件主题：{POOL_TOPIC}")

    if start > latest:
        log("没有新块需要扫描")
        return

    # 限制本次处理的最大区块数量。
    end = min(
        latest,
        start + MAX_BLOCKS_PER_RUN - 1,
    )

    log(f"本次扫描范围：{start}-{end}")

    # 如果扫描失败，不更新 last_block，
    # 下次运行会从未完成的区块继续。
    events = discover(start, end)

    log(f"发现新池事件：{len(events)}")

    seen = set(state.get("seen", []))
    pushed = 0
    processed = 0

    for event in events:
        event_id = (
            f"{event['tx']}:{event['log_index']}"
        )

        if event_id in seen:
            continue

        seen.add(event_id)
        processed += 1

        for token in (
            event["token0"],
            event["token1"],
        ):
            pairs = market_pairs(token)

            candidates = [
                pair for pair in pairs
                if pair["liquidity"] >= MIN_LIQUIDITY_USD
                and pair["volume"] >= MIN_VOLUME_24H_USD
            ]

            if not candidates:
                continue

            candidates.sort(
                key=lambda pair: (
                    pair["liquidity"],
                    pair["volume"],
                ),
                reverse=True,
            )

            pair = candidates[0]

            title = (
                f"新池候选："
                f"{pair['symbol'] or token[:10]}"
            )

            body = (
                f"DEX：{event['dex']}\n"
                f"代币地址：{token}\n"
                f"池地址：{event['pool']}\n"
                f"名称：{pair['name']}\n"
                f"代号：{pair['symbol']}\n"
                f"交易对：{pair['quote']}\n"
                f"价格 USD：{pair['price']}\n"
                f"流动性 USD：{pair['liquidity']:.2f}\n"
                f"24 小时成交量 USD：{pair['volume']:.2f}\n"
                f"行情链接：{pair['url']}\n"
                f"区块：{event['block']}\n"
                f"交易：{event['tx']}\n\n"
                "注意：这只是候选提醒，不代表安全或建议买入。\n"
                "尚未验证合约税费、貔貅风险、持币集中度及 LP 锁定。\n"
                "本程序只读，不会自动买卖。"
            )

            if send_wechat(title, body):
                pushed += 1

            if pushed >= MAX_PUSHES:
                break

        if pushed >= MAX_PUSHES:
            break

    # 只有扫描完整结束后才推进进度。
    # 如果中途报错，程序会退出，不会错误跳过未扫区块。
    state["last_block"] = end
    state["seen"] = list(seen)[-10000:]
    save_state(state)

    log(
        f"扫描完成：区块 {start}-{end}，"
        f"新池事件 {len(events)}，"
        f"处理事件 {processed}，"
        f"成功推送 {pushed} 条"
    )

    if end < latest:
        log(
            f"本次只处理到 {end}，"
            f"剩余区块将在后续运行继续扫描"
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"程序停止：{exc}")
        raise SystemExit(1)
