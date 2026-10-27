
import json
import os
import sys
import time
from pathlib import Path

import requests
from web3 import Web3

# ============================================================
# Robinhood Chain read-only scanner
# Only scans, scores and sends alerts. Never trades.
# ============================================================

CHAIN_ID = 4663
CHAIN_SLUG = "robinhood"

RPC_URL = os.getenv("ALCHEMY_RPC_URL", "").strip()
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "").strip()

STATE_FILE = Path("state.json")

# Alchemy free-tier eth_getLogs range: keep each request <= 10 blocks.
CHUNK_SIZE = 10

# Limit each GitHub Actions run so it can finish and save progress.
# Increase only if your RPC quota can handle the extra requests.
MAX_BLOCKS_PER_RUN = 300

REQUEST_TIMEOUT = 20
RPC_MIN_INTERVAL = 0.8
MAX_RETRIES = 5
MAX_PUSHES = 8

# Candidate thresholds; these are filters, not safety guarantees.
MIN_LIQUIDITY_USD = 300_000
MIN_VOLUME_24H_USD = 100_000
MIN_TXNS_24H = 20
MIN_BUY_SELL_RATIO = 0.80
MAX_VOLUME_LIQUIDITY_RATIO = 5.0
MAX_PAIR_AGE_DAYS = 3

# Verified/identified V3 factory addresses.
# New factories must be independently verified before adding them.
FACTORIES = {
    "Uniswap V3": "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA",
    "Goo Exchange V3": "0x221A6239E40709792b0d4bdc140fA36158CD41C7",
}

POOL_TOPIC = Web3.to_hex(
    Web3.keccak(text="PoolCreated(address,address,uint24,int24,address)")
)

session = requests.Session()
session.headers.update({"User-Agent": "RobinhoodReadOnlyScanner/2.0"})
last_rpc_request = 0.0


def log(message):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), message, flush=True)


def rpc(method, params):
    global last_rpc_request

    if not RPC_URL:
        raise RuntimeError(
            "缺少 ALCHEMY_RPC_URL。请检查 GitHub 仓库 Settings > Secrets "
            "and variables > Actions。"
        )

    for attempt in range(MAX_RETRIES):
        delay = RPC_MIN_INTERVAL - (time.monotonic() - last_rpc_request)
        if delay > 0:
            time.sleep(delay)

        try:
            response = session.post(
                RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params,
                },
                timeout=REQUEST_TIMEOUT,
            )
            last_rpc_request = time.monotonic()

            if response.status_code == 429:
                wait = min(3 * (attempt + 1), 20)
                log(f"RPC HTTP 429 限流，{wait} 秒后重试")
                time.sleep(wait)
                continue

            if response.status_code >= 500:
                wait = min(2 * (attempt + 1), 15)
                log(f"RPC HTTP {response.status_code}，{wait} 秒后重试")
                time.sleep(wait)
                continue

            if response.status_code == 400:
                raise RuntimeError(
                    f"RPC HTTP 400，方法={method}，请求参数={params}，"
                    f"返回={response.text[:500]}"
                )

            response.raise_for_status()
            payload = response.json()

            if payload.get("error"):
                err = payload["error"]
                message = str(err.get("message", err))
                code = err.get("code")

                if code == -32005 or "rate limit" in message.lower():
                    wait = min(3 * (attempt + 1), 20)
                    log(f"RPC 限流：{message}；等待 {wait} 秒")
                    time.sleep(wait)
                    continue

                # Invalid parameters will not improve by retrying.
                raise RuntimeError(
                    f"RPC 错误 method={method}, code={code}, "
                    f"message={message}, params={params}"
                )

            if "result" not in payload:
                raise RuntimeError(f"RPC 返回缺少 result：{payload}")

            return payload["result"]

        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"RPC 网络请求失败：{exc}") from exc
            wait = min(2 * (attempt + 1), 15)
            log(f"RPC 网络异常，{wait} 秒后重试：{exc}")
            time.sleep(wait)

    raise RuntimeError(f"RPC {method} 重试耗尽，本次扫描停止")


def load_state():
    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(state, dict):
            return {
                "last_block": state.get("last_block"),
                "seen": list(state.get("seen", [])),
            }
    except (OSError, ValueError, TypeError):
        pass

    return {"last_block": None, "seen": []}


def save_state(state):
    state["seen"] = list(dict.fromkeys(state["seen"]))[-10000:]
    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp.replace(STATE_FILE)


def address_from_topic(topic):
    value = str(topic)
    if not value.startswith("0x"):
        value = "0x" + value
    if len(value) != 66:
        raise ValueError(f"非法地址 topic：{value}")
    return Web3.to_checksum_address("0x" + value[-40:])


def address_from_data(data, word_index):
    raw = data[2:] if str(data).startswith("0x") else str(data)
    start = word_index * 64
    word = raw[start:start + 64]

    if len(word) != 64:
        return ""

    address_hex = word[-40:]
    if int(address_hex, 16) == 0:
        return ""

    return Web3.to_checksum_address("0x" + address_hex)


def get_pool_events(start_block, end_block):
    if end_block < start_block:
        return []

    if end_block - start_block + 1 > CHUNK_SIZE:
        raise ValueError("eth_getLogs 查询区块范围超过限制")

    # One log query for all configured factories, instead of one per factory.
    factory_addresses = [
        Web3.to_checksum_address(address)
        for address in FACTORIES.values()
    ]

    logs = rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
            "address": factory_addresses,
            "topics": [POOL_TOPIC],
        }],
    ) or []

    events = []
    factory_names = {
        address.lower(): name for name, address in FACTORIES.items()
    }

    for item in logs:
        topics = item.get("topics", [])
        if len(topics) < 4:
            continue

        dex_name = factory_names.get(str(item.get("address", "")).lower())
        if not dex_name:
            continue

        try:
            token0 = address_from_topic(topics[1])
            token1 = address_from_topic(topics[2])

            # Uniswap V3 PoolCreated data:
            # word 0 = tickSpacing; word 1 = pool address.
            pool_address = address_from_data(item.get("data", "0x"), 1)

            if not pool_address or token0.lower() == token1.lower():
                continue

            events.append({
                "dex": dex_name,
                "token0": token0,
                "token1": token1,
                "pool": pool_address,
                "block": int(item.get("blockNumber", "0x0"), 16),
                "tx": item.get("transactionHash", ""),
                "log_index": int(item.get("logIndex", "0x0"), 16),
            })
        except (ValueError, TypeError) as exc:
            log(f"跳过格式异常的池事件：{exc}")

    return events


def get_market_pairs(tokens):
    """Batch query up to 30 token addresses per request."""
    unique = list(dict.fromkeys(t.lower() for t in tokens))
    pairs = []

    for offset in range(0, len(unique), 30):
        batch = unique[offset:offset + 30]
        if not batch:
            continue

        url = (
            "https://api.dexscreener.com/tokens/v1/"
            + CHAIN_SLUG + "/" + ",".join(batch)
        )

        for attempt in range(4):
            try:
                response = session.get(url, timeout=REQUEST_TIMEOUT)

                if response.status_code == 429:
                    wait = 2 * (attempt + 1)
                    log(f"行情 API 限流，等待 {wait} 秒")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                data = response.json()

                if isinstance(data, list):
                    pairs.extend(
                        pair for pair in data
                        if str(pair.get("chainId", "")).lower() == CHAIN_SLUG
                    )
                break

            except (requests.RequestException, ValueError) as exc:
                if attempt == 3:
                    log(f"批量行情查询失败：{exc}")
                else:
                    time.sleep(2 * (attempt + 1))

    return pairs


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def evaluate_pair(pair, token_address, now_ms):
    """Return (score, reasons); score=None means rejected."""
    base = pair.get("baseToken") or {}
    if str(base.get("address", "")).lower() != token_address.lower():
        return None, ["不是该代币作为 base token 的交易对"]

    liquidity = safe_float((pair.get("liquidity") or {}).get("usd"))
    volume = safe_float((pair.get("volume") or {}).get("h24"))
    txns = (pair.get("txns") or {}).get("h24") or {}
    buys = int(safe_float(txns.get("buys")))
    sells = int(safe_float(txns.get("sells")))
    total_txns = buys + sells

    if liquidity < MIN_LIQUIDITY_USD:
        return None, ["流动性不足"]
    if volume < MIN_VOLUME_24H_USD:
        return None, ["24 小时成交量不足"]
    if total_txns < MIN_TXNS_24H:
        return None, ["24 小时交易笔数不足"]

    buy_sell_ratio = buys / max(sells, 1)
    if buy_sell_ratio < MIN_BUY_SELL_RATIO:
        return None, ["卖出笔数明显偏多"]

    volume_liquidity_ratio = volume / max(liquidity, 1)
    if volume_liquidity_ratio > MAX_VOLUME_LIQUIDITY_RATIO:
        return None, ["成交量/流动性比例异常偏高，需警惕刷量"]

    created = pair.get("pairCreatedAt")
    age_hours = None
    if created:
        age_hours = max(0, (now_ms - safe_float(created)) / 3_600_000)
        if age_hours > MAX_PAIR_AGE_DAYS * 24:
            return None, ["交易池超过新池观察期限"]

    # Transparent heuristic score, not a prediction of future returns.
    score = 0
    reasons = []

    score += 30 if liquidity >= 500_000 else 22
    reasons.append(f"流动性 ${liquidity:,.0f}")

    score += 25 if volume >= 300_000 else 18
    reasons.append(f"24h 成交量 ${volume:,.0f}")

    score += 15 if buy_sell_ratio >= 1.2 else 10
    reasons.append(f"买/卖笔数 {buys}/{sells}")

    score += 15 if 0.10 <= volume_liquidity_ratio <= 2.0 else 8
    reasons.append(f"成交量/流动性 {volume_liquidity_ratio:.2f}")

    if age_hours is not None:
        score += 15 if age_hours <= 24 else 8
        reasons.append(f"池龄 {age_hours:.1f} 小时")
    else:
        reasons.append("未取得池创建时间")

    return min(score, 100), reasons


def self_test():
    now_ms = int(time.time() * 1000)

    def sample(liquidity, volume, buys, sells, ratio=None):
        if ratio is None:
            ratio = volume / max(liquidity, 1)
        return {
            "baseToken": {"address": "0x" + "1" * 40},
            "liquidity": {"usd": liquidity},
            "volume": {"h24": volume},
            "txns": {"h24": {"buys": buys, "sells": sells}},
            "pairCreatedAt": now_ms - 2 * 60 * 60 * 1000,
        }

    tests = [
        ("合格候选应通过", sample(600000, 300000, 150, 100), True),
        ("低流动性应拒绝", sample(10000, 300000, 150, 100), False),
        ("成交量不足应拒绝", sample(600000, 1000, 150, 100), False),
        ("卖出过多应拒绝", sample(600000, 300000, 20, 100), False),
        ("异常高成交量比例应拒绝",
         sample(300000, 2000000, 150, 100), False),
    ]

    passed = 0
    for name, pair, should_pass in tests:
        score, _ = evaluate_pair(
            pair, "0x" + "1" * 40, now_ms
        )
        actual_pass = score is not None
        ok = actual_pass == should_pass
        passed += int(ok)
        log(f"筛选自测 {'通过' if ok else '失败'}：{name}")

    log(f"筛选规则自测结果：{passed}/{len(tests)}")
    if passed != len(tests):
        raise RuntimeError("筛选规则自测未全部通过")


def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        log("缺少 SERVER_CHAN_KEY，无法发送微信推送")
        return False

    try:
        response = session.post(
            f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send",
            data={"title": title[:100], "desp": body},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        result = response.json()

        success = (
            result.get("code") == 0
            or (result.get("data") or {}).get("errno") == 0
        )
        log("微信推送成功" if success else f"微信推送失败：{result}")
        return success

    except Exception as exc:
        log(f"微信推送请求失败：{exc}")
        return False


def main():
    self_test()

    actual_chain_id = int(rpc("eth_chainId", []), 16)
    if actual_chain_id != CHAIN_ID:
        raise RuntimeError(
            f"链 ID 不匹配：期望 {CHAIN_ID}，实际 {actual_chain_id}"
        )

    latest = int(rpc("eth_blockNumber", []), 16)
    state = load_state()
    previous = state.get("last_block")

    if previous is None:
        # First run: scan only the most recent 300 blocks.
        start = max(0, latest - MAX_BLOCKS_PER_RUN + 1)
    else:
        start = int(previous) + 1

    if start > latest:
        log(f"没有新块。当前最新区块：{latest}")
        return

    end = min(latest, start + MAX_BLOCKS_PER_RUN - 1)

    log(f"Robinhood Chain 连接成功，最新区块：{latest}")
    log(f"事件主题：{POOL_TOPIC}")
    log(f"本次扫描范围：{start}-{end}，区块数：{end - start + 1}")

    all_events = []
    cursor = start

    # Save progress after each successful chunk.
    while cursor <= end:
        chunk_end = min(end, cursor + CHUNK_SIZE - 1)
        log(f"正在查询区块 {cursor}-{chunk_end}")

        events = get_pool_events(cursor, chunk_end)
        all_events.extend(events)

        # This cursor means logs for this chunk were queried successfully.
        state["last_block"] = chunk_end
        save_state(state)
        cursor = chunk_end + 1

    log(f"发现新池事件：{len(all_events)}")

    seen = set(state.get("seen", []))
    new_events = []

    for event in all_events:
        event_id = f"{event['tx']}:{event['log_index']}"
        if event_id not in seen:
            new_events.append((event_id, event))

    tokens = []
    for _, event in new_events:
        tokens.extend([event["token0"], event["token1"]])

    market_pairs = get_market_pairs(tokens)
    now_ms = int(time.time() * 1000)
    pushed = 0

    for event_id, event in new_events:
        token_addresses = [event["token0"], event["token1"]]

        for token in token_addresses:
            candidates = []

            for pair in market_pairs:
                score, reasons = evaluate_pair(pair, token, now_ms)
                if score is not None:
                    candidates.append((score, pair, reasons))

            if not candidates:
                continue

            candidates.sort(key=lambda item: item[0], reverse=True)
            score, pair, reasons = candidates[0]
            base = pair.get("baseToken") or {}
            symbol = base.get("symbol") or token[:10]

            body = (
                f"评分：{score}/100\n"
                f"DEX：{event['dex']}\n"
                f"代币地址：{token}\n"
                f"池地址：{event['pool']}\n"
                f"名称：{base.get('name', '')}\n"
                f"代号：{symbol}\n"
                f"价格 USD：{pair.get('priceUsd', '?')}\n"
                f"流动性 USD：{safe_float((pair.get('liquidity') or {}).get('usd')):,.2f}\n"
                f"24h 成交量 USD：{safe_float((pair.get('volume') or {}).get('h24')):,.2f}\n"
                f"24h 买/卖笔数："
                f"{(pair.get('txns') or {}).get('h24', {}).get('buys', '?')}/"
                f"{(pair.get('txns') or {}).get('h24', {}).get('sells', '?')}\n"
                f"筛选依据：\n- " + "\n- ".join(reasons) + "\n"
                f"行情链接：{pair.get('url', '')}\n"
                f"区块：{event['block']}\n"
                f"交易：{event['tx']}\n\n"
                "风险声明：这是数据筛选提醒，不是买入建议。"
                "尚未验证合约权限、买卖税、貔貅风险、持币集中度及 LP 锁定。"
                "本程序只读，不会自动买卖。"
            )

            if send_wechat(f"新池候选 {symbol}｜评分{score}", body):
                pushed += 1

            if pushed >= MAX_PUSHES:
                break

        seen.add(event_id)

        if pushed >= MAX_PUSHES:
            break

    state["seen"] = list(seen)[-10000:]
    save_state(state)

    log(
        f"扫描结束：新池事件 {len(all_events)}，"
        f"待处理事件 {len(new_events)}，成功推送 {pushed} 条"
    )

    if end < latest:
        log(
            f"本次处理到区块 {end}，最新区块为 {latest}。"
            "剩余区块将在后续运行继续扫描。"
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"程序停止：{exc}")
        raise SystemExit(1)
