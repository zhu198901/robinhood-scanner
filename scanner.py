
import json
import os
import sys
import time
from pathlib import Path

import requests
from web3 import Web3


# ============================================================
# Robinhood Chain Scanner
# 只扫描、筛选、推送候选币，不会自动买入
# ============================================================

CHAIN_ID = 4663
RPC_URL = os.getenv("ALCHEMY_RPC_URL", "").strip()
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "").strip()

STATE_FILE = Path("state.json")

# Alchemy 免费档：每次 eth_getLogs 最多查询 10 个区块
CHUNK_SIZE = 10

# 每次 GitHub Actions 尽量扫描 3000 个区块
MAX_BLOCKS_PER_RUN = 3000

REQUEST_TIMEOUT = 20
RPC_MIN_INTERVAL = 0.25
MAX_RETRIES = 5
MAX_PUSHES = 8

# 当落后超过这个区块数时，跳到最新区块附近恢复实时监控。
# 注意：跳转会跳过未扫描的历史区块。
MAX_LAG_BLOCKS = 50000
STARTUP_LOOKBACK_BLOCKS = 300

# 候选币筛选条件
MIN_LIQUIDITY_USD = 300000
MIN_VOLUME_24H_USD = 100000
MIN_TXNS_24H = 20
MIN_BUY_SELL_RATIO = 0.80
MAX_VOLUME_LIQUIDITY_RATIO = 5.0
MAX_PAIR_AGE_DAYS = 3

FACTORIES = [
    Web3.to_checksum_address(
        "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA"
    ),
    Web3.to_checksum_address(
        "0x221A6239E40709792b0d4bdc140fA36158CD41C7"
    ),
]

POOL_TOPIC = Web3.to_hex(
    Web3.keccak(
        text="PoolCreated(address,address,uint24,int24,address)"
    )
)

session = requests.Session()
last_rpc_request = 0.0


# ============================================================
# RPC
# ============================================================

def rpc(method, params):
    global last_rpc_request

    if not RPC_URL:
        raise RuntimeError(
            "缺少 ALCHEMY_RPC_URL，请检查 GitHub Secrets。"
        )

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    for attempt in range(MAX_RETRIES):
        elapsed = time.monotonic() - last_rpc_request
        if elapsed < RPC_MIN_INTERVAL:
            time.sleep(RPC_MIN_INTERVAL - elapsed)

        last_rpc_request = time.monotonic()

        try:
            response = session.post(
                RPC_URL,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                wait = min(2 ** attempt, 20)
                print(f"RPC 限流，{wait} 秒后重试")
                time.sleep(wait)
                continue

            if response.status_code >= 500:
                wait = min(2 ** attempt, 20)
                print(f"RPC 服务错误 {response.status_code}，重试")
                time.sleep(wait)
                continue

            response.raise_for_status()
            data = response.json()

            if "error" in data:
                error = data["error"]
                message = str(error.get("message", error))

                if (
                    "-32005" in str(error)
                    or "rate limit" in message.lower()
                    or "too many requests" in message.lower()
                ):
                    wait = min(2 ** attempt, 20)
                    print(f"RPC 暂时限流，{wait} 秒后重试")
                    time.sleep(wait)
                    continue

                raise RuntimeError(
                    f"RPC {method} 失败：{message}"
                )

            if "result" not in data:
                raise RuntimeError(
                    f"RPC {method} 返回格式异常"
                )

            return data["result"]

        except requests.RequestException as exc:
            wait = min(2 ** attempt, 20)
            print(
                f"RPC 网络错误，第 {attempt + 1} 次尝试：{exc}"
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)

        except ValueError as exc:
            wait = min(2 ** attempt, 20)
            print(f"RPC 返回内容无法解析：{exc}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)

    raise RuntimeError(f"RPC {method} 多次失败，停止本轮扫描")


# ============================================================
# 状态保存
# ============================================================

def load_state():
    if not STATE_FILE.exists():
        return {
            "last_block": None,
            "seen": [],
            "pending": [],
        }

    try:
        with STATE_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)

        return {
            "last_block": data.get("last_block"),
            "seen": list(data.get("seen", [])),
            "pending": list(data.get("pending", [])),
        }

    except (OSError, ValueError, TypeError) as exc:
        print(f"读取扫描状态失败：{exc}")
        print("为避免误用损坏状态，将从最新区块附近启动。")
        return {
            "last_block": None,
            "seen": [],
            "pending": [],
        }


def save_state(state):
    state["seen"] = list(dict.fromkeys(state["seen"]))[-20000:]
    state["pending"] = list(
        dict.fromkeys(state["pending"])
    )[-10000:]

    temp_file = STATE_FILE.with_suffix(".tmp")

    with temp_file.open("w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False)

    temp_file.replace(STATE_FILE)


# ============================================================
# 区块事件解析
# ============================================================

def address_from_topic(topic):
    value = topic[-40:]
    return Web3.to_checksum_address("0x" + value)


def address_from_data(data, word_index):
    raw = data[2:] if data.startswith("0x") else data
    start = word_index * 64
    word = raw[start:start + 64]

    if len(word) != 64:
        return None

    return Web3.to_checksum_address("0x" + word[-40:])


def get_pool_events(start_block, end_block):
    if end_block < start_block:
        return []

    if end_block - start_block + 1 > CHUNK_SIZE:
        raise ValueError(
            f"查询范围超过限制：{start_block} - {end_block}"
        )

    logs = rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
            "address": FACTORIES,
            "topics": [POOL_TOPIC],
        }],
    )

    events = []

    for log in logs:
        topics = log.get("topics", [])

        if len(topics) < 3:
            continue

        token0 = address_from_topic(topics[1])
        token1 = address_from_topic(topics[2])

        # Uniswap V3 PoolCreated 的 data：
        # word 0 = tickSpacing，word 1 = pool address
        pool_address = address_from_data(log.get("data", ""), 1)

        if not pool_address:
            continue

        event = {
            "event_id": (
                str(log.get("transactionHash", ""))
                + ":"
                + str(log.get("logIndex", ""))
            ),
            "token0": token0,
            "token1": token1,
            "pool": pool_address,
            "block": int(log["blockNumber"], 16),
            "tx": log.get("transactionHash", ""),
        }

        events.append(event)

    return events


# ============================================================
# DexScreener 行情查询
# ============================================================

def get_market_pairs(tokens):
    tokens = list(dict.fromkeys(
        token.lower() for token in tokens if token
    ))

    if not tokens:
        return [], True

    all_pairs = []
    success_count = 0

    for offset in range(0, len(tokens), 30):
        batch = tokens[offset:offset + 30]
        url = (
            "https://api.dexscreener.com/tokens/v1/robinhood/"
            + ",".join(batch)
        )

        succeeded = False

        for attempt in range(4):
            try:
                response = session.get(
                    url,
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code == 429:
                    time.sleep(min(2 ** attempt, 15))
                    continue

                response.raise_for_status()
                result = response.json()

                if not isinstance(result, list):
                    raise ValueError("行情接口返回格式异常")

                all_pairs.extend(result)
                success_count += 1
                succeeded = True
                break

            except (
                requests.RequestException,
                ValueError,
            ) as exc:
                print(f"DexScreener 查询失败：{exc}")
                if attempt < 3:
                    time.sleep(min(2 ** attempt, 15))

        if not succeeded:
            print("本批行情查询失败，将保留相关事件等待重试。")

    all_succeeded = success_count == (
        (len(tokens) + 29) // 30
    )

    return all_pairs, all_succeeded


# ============================================================
# 候选币筛选
# ============================================================

def evaluate_pair(pair, token_address, now_ms):
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}

    if (base.get("address") or "").lower() != token_address.lower():
        return None

    liquidity = float(
        (pair.get("liquidity") or {}).get("usd") or 0
    )
    volume = float(
        (pair.get("volume") or {}).get("h24") or 0
    )

    txns = pair.get("txns") or {}
    h24 = txns.get("h24") or {}

    buys = int(h24.get("buys") or 0)
    sells = int(h24.get("sells") or 0)
    total_txns = buys + sells

    if liquidity < MIN_LIQUIDITY_USD:
        return None

    if volume < MIN_VOLUME_24H_USD:
        return None

    if total_txns < MIN_TXNS_24H:
        return None

    # 防止成交量远高于池子流动性，出现异常刷量风险
    if volume / max(liquidity, 1) > MAX_VOLUME_LIQUIDITY_RATIO:
        return None

    # 买卖比：买单数 / 卖单数
    if sells == 0:
        buy_sell_ratio = float(buys) if buys > 0 else 0
    else:
        buy_sell_ratio = buys / sells

    if buy_sell_ratio < MIN_BUY_SELL_RATIO:
        return None

    pair_created = pair.get("pairCreatedAt")
    if not pair_created:
        return None

    age_days = (now_ms - int(pair_created)) / 86400000

    if age_days < 0 or age_days > MAX_PAIR_AGE_DAYS:
        return None

    price = pair.get("priceUsd") or "未知"
    pair_url = pair.get("url") or ""

    score = 0
    score += min(liquidity / MIN_LIQUIDITY_USD * 15, 25)
    score += min(volume / MIN_VOLUME_24H_USD * 10, 25)
    score += min(total_txns / 20 * 5, 20)
    score += min(buy_sell_ratio * 10, 20)
    score += max(10 - age_days * 3, 0)

    return {
        "name": base.get("name") or "未知代币",
        "symbol": base.get("symbol") or "未知",
        "address": base.get("address") or token_address,
        "quote": quote.get("symbol") or "",
        "price": price,
        "liquidity": liquidity,
        "volume": volume,
        "buys": buys,
        "sells": sells,
        "age_days": age_days,
        "score": round(score, 1),
        "url": pair_url,
    }


# ============================================================
# 微信推送
# ============================================================

def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        print("缺少 SERVER_CHAN_KEY，无法推送微信。")
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
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()
        result = response.json()

        if result.get("code") == 0:
            print(f"微信推送成功：{title}")
            return True

        print(f"微信推送失败：{result}")
        return False

    except (requests.RequestException, ValueError) as exc:
        print(f"微信推送异常：{exc}")
        return False


def format_candidate(candidate, event):
    return (
        f"代币：{candidate['name']} ({candidate['symbol']})\n"
        f"价格：${candidate['price']}\n"
        f"流动性：${candidate['liquidity']:,.0f}\n"
        f"24小时成交量：${candidate['volume']:,.0f}\n"
        f"24小时买单/卖单："
        f"{candidate['buys']}/{candidate['sells']}\n"
        f"池子年龄：{candidate['age_days']:.2f} 天\n"
        f"筛选评分：{candidate['score']}/100\n"
        f"代币合约：{candidate['address']}\n"
        f"池子地址：{event['pool']}\n"
        f"交易链接：{candidate['url']}\n\n"
        f"注意：这是筛选候选，不代表合约安全或建议直接买入。"
    )


# ============================================================
# 启动检查
# ============================================================

def self_test():
    assert CHUNK_SIZE == 10
    assert len(FACTORIES) == 2
    assert POOL_TOPIC.startswith("0x")
    assert MAX_BLOCKS_PER_RUN >= CHUNK_SIZE
    print("基础自检通过。")


# ============================================================
# 主程序
# ============================================================

def main():
    self_test()

    if not RPC_URL:
        print("错误：GitHub Secrets 中缺少 ALCHEMY_RPC_URL。")
        sys.exit(1)

    chain_id = int(rpc("eth_chainId", []), 16)

    if chain_id != CHAIN_ID:
        print(
            f"错误：RPC 链 ID 为 {chain_id}，"
            f"预期为 {CHAIN_ID}。"
        )
        sys.exit(1)

    latest_block = int(rpc("eth_blockNumber", []), 16)
    state = load_state()

    last_block = state.get("last_block")

    print(f"Robinhood Chain 最新区块：{latest_block}")
    print(f"上次完成区块：{last_block}")

    if last_block is None:
        start_block = max(
            0,
            latest_block - STARTUP_LOOKBACK_BLOCKS + 1,
        )
        print(
            f"首次启动：从最近 {STARTUP_LOOKBACK_BLOCKS} "
            f"个区块开始扫描。"
        )

    else:
        start_block = int(last_block) + 1
        lag = latest_block - int(last_block)

        if lag > MAX_LAG_BLOCKS:
            start_block = max(
                0,
                latest_block - STARTUP_LOOKBACK_BLOCKS + 1,
            )
            print(
                f"警告：历史积压 {lag:,} 个区块，"
                f"超过 {MAX_LAG_BLOCKS:,}。"
            )
            print(
                "为恢复实时监控，本轮将跳过旧积压，"
                f"从区块 {start_block} 附近开始。"
            )

    end_block = min(
        latest_block,
        start_block + MAX_BLOCKS_PER_RUN - 1,
    )

    if start_block > end_block:
        print("当前没有需要扫描的区块。")
        return

    print(f"本轮扫描范围：{start_block} - {end_block}")

    events = []
    cursor = start_block

    # 逐段扫描；每次 eth_getLogs 最多 10 个区块
    while cursor <= end_block:
        chunk_end = min(
            cursor + CHUNK_SIZE - 1,
            end_block,
        )

        chunk_events = get_pool_events(cursor, chunk_end)
        events.extend(chunk_events)

        # 每个区块段扫描成功后才保存进度
        state["last_block"] = chunk_end
        save_state(state)

        print(
            f"已扫描 {cursor} - {chunk_end}，"
            f"发现 {len(chunk_events)} 个新池事件。"
        )

        cursor = chunk_end + 1

    print(f"本轮发现池创建事件：{len(events)}")

    seen = set(state.get("seen", []))
    pending = set(state.get("pending", []))

    new_events = []
    event_by_id = {}

    for event in events:
        event_id = event["event_id"]

        if event_id in seen:
            continue

        event_by_id[event_id] = event
        new_events.append(event)

    # 之前行情查询失败的事件也会再次尝试
    for event_id in list(pending):
        if event_id not in event_by_id and event_id not in seen:
            # 历史事件详情不存储时，无法从 pending 恢复完整数据；
            # 当前轮只处理仍在本轮扫描范围内的事件。
            continue

    if not new_events:
        print("本轮没有未处理的新池事件。")
        save_state(state)
        return

    tokens = []

    for event in new_events:
        tokens.extend([event["token0"], event["token1"]])

    market_pairs, market_ok = get_market_pairs(tokens)

    if not market_ok:
        print(
            "行情接口部分失败。本轮不把相关事件标记为完成，"
            "避免查询失败造成漏处理。"
        )

    now_ms = int(time.time() * 1000)
    pushed = 0

    # 建立代币地址到行情池子的映射
    pairs_by_token = {}

    for pair in market_pairs:
        base = pair.get("baseToken") or {}
        address = (base.get("address") or "").lower()

        if address:
            pairs_by_token.setdefault(address, []).append(pair)

    for event in new_events:
        event_id = event["event_id"]

        if event_id in seen:
            continue

        token_candidates = []
        event_has_failed_lookup = False

        for token in [event["token0"], event["token1"]]:
            matching_pairs = pairs_by_token.get(token.lower(), [])

            if not matching_pairs:
                # 如果整批行情查询失败，不能断定这个币没有行情
                if not market_ok:
                    event_has_failed_lookup = True
                continue

            for pair in matching_pairs:
                candidate = evaluate_pair(pair, token, now_ms)

                if candidate:
                    token_candidates.append(candidate)

        if event_has_failed_lookup:
            pending.add(event_id)
            continue

        if token_candidates and pushed < MAX_PUSHES:
            best = max(
                token_candidates,
                key=lambda item: item["score"],
            )

            title = (
                f"Robinhood候选币："
                f"{best['symbol']}｜评分{best['score']}"
            )

            body = format_candidate(best, event)

            if send_wechat(title, body):
                pushed += 1
                seen.add(event_id)
                pending.discard(event_id)
            else:
                # 推送失败时保留待处理状态，下次重试
                pending.add(event_id)

        elif token_candidates and pushed >= MAX_PUSHES:
            # 达到推送上限的事件不标记完成，避免永久漏掉
            pending.add(event_id)

        else:
            # 行情查询成功，但没有符合筛选条件的池子
            seen.add(event_id)
            pending.discard(event_id)

    state["seen"] = list(seen)[-20000:]
    state["pending"] = list(pending)[-10000:]
    save_state(state)

    print(f"本轮成功推送：{pushed}")
    print(f"已记录事件数：{len(state['seen'])}")
    print(f"待重试事件数：{len(state['pending'])}")
    print(f"本轮完成区块：{state['last_block']}")

    if end_block < latest_block:
        print(
            f"仍有 {latest_block - end_block:,} 个区块"
            "尚未扫描，将在后续运行继续处理。"
        )

    print("扫描结束。")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"本轮扫描异常：{exc}")
        sys.exit(1)
