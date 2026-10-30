import json
import os
import sys
import time
from pathlib import Path

import requests
from web3 import Web3

# ============================================================
# Robinhood Chain Scanner
# Read-only: scan new V3 pools and send candidate alerts to WeChat.
# ============================================================

RPC_URL = os.getenv("ALCHEMY_RPC_URL", "").strip()
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "").strip()

CHAIN_ID = 4663
STATE_FILE = Path("state.json")

FACTORIES = [
    Web3.to_checksum_address(
        "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA"
    ),
    Web3.to_checksum_address(
        "0x221A6239E40709792b0d4bdc140fA36158CD41C7"
    ),
]

# V3 PoolCreated(address,address,uint24,int24,address)
_topic_hash = Web3.keccak(
    text="PoolCreated(address,address,uint24,int24,address)"
).hex()

POOL_CREATED_TOPIC = (
    _topic_hash
    if _topic_hash.startswith("0x")
    else "0x" + _topic_hash
)

# Scan limits
CHUNK_SIZE = 10
MAX_BLOCKS_PER_RUN = 300
MAX_LAG_BLOCKS = 200000
MAX_PUSHES = 8
MAX_PENDING_EVENTS = 3000
MAX_SEEN_EVENTS = 10000

# RPC/API request pacing
RPC_MIN_INTERVAL = 0.8
REQUEST_TIMEOUT = 20
MAX_RETRIES = 5

# Candidate filters
MIN_LIQUIDITY_USD = 300000
MIN_VOLUME_24H_USD = 100000
MIN_TXNS_24H = 20
MIN_BUY_SELL_RATIO = 0.80
MAX_VOLUME_LIQUIDITY_RATIO = 5.0
MAX_PAIR_AGE_DAYS = 3

# Pending events with no market data are retried for up to 24 hours.
PENDING_MAX_AGE_SECONDS = 24 * 60 * 60

session = requests.Session()
last_rpc_request = 0.0


def log(message):
    print(
        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}",
        flush=True,
    )


def rpc(method, params=None):
    global last_rpc_request

    if not RPC_URL:
        raise RuntimeError(
            "Missing GitHub Secret: ALCHEMY_RPC_URL"
        )

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or [],
    }

    for attempt in range(MAX_RETRIES):
        wait = RPC_MIN_INTERVAL - (
            time.monotonic() - last_rpc_request
        )

        if wait > 0:
            time.sleep(wait)

        try:
            response = session.post(
                RPC_URL,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )

            last_rpc_request = time.monotonic()

            if (
                response.status_code == 429
                or response.status_code >= 500
            ):
                delay = min(2 ** attempt, 20)
                log(
                    f"RPC HTTP {response.status_code}; "
                    f"retry in {delay}s"
                )
                time.sleep(delay)
                continue

            response.raise_for_status()
            body = response.json()

            if body.get("error"):
                error = body["error"]
                message = str(error.get("message", error))
                code = error.get("code")

                if (
                    code == -32005
                    or "rate limit" in message.lower()
                ):
                    delay = min(2 ** attempt, 20)
                    log(
                        f"RPC rate limit; retry in {delay}s"
                    )
                    time.sleep(delay)
                    continue

                raise RuntimeError(
                    f"RPC {method} error: {error}"
                )

            if "result" not in body:
                raise RuntimeError(
                    f"RPC {method}: missing result"
                )

            return body["result"]

        except (
            requests.RequestException,
            ValueError,
        ) as exc:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(
                    f"RPC {method} failed: {exc}"
                ) from exc

            delay = min(2 ** attempt, 20)
            log(
                f"RPC request failed: {exc}; "
                f"retry in {delay}s"
            )
            time.sleep(delay)

    raise RuntimeError(
        f"RPC {method} exhausted retries"
    )


def load_state():
    if not STATE_FILE.exists():
        return {
            "last_block": None,
            "seen": [],
            "pending": [],
            "skipped_ranges": [],
        }

    try:
        data = json.loads(
            STATE_FILE.read_text(encoding="utf-8")
        )

        if not isinstance(data, dict):
            raise ValueError(
                "state root is not an object"
            )

        data.setdefault("last_block", None)
        data.setdefault("seen", [])
        data.setdefault("pending", [])
        data.setdefault("skipped_ranges", [])

        if not isinstance(data["seen"], list):
            data["seen"] = []

        if not isinstance(data["pending"], list):
            data["pending"] = []

        if not isinstance(data["skipped_ranges"], list):
            data["skipped_ranges"] = []

        return data

    except Exception as exc:
        raise RuntimeError(
            f"Cannot read {STATE_FILE}: {exc}. "
            "Stop to protect the existing scan state."
        ) from exc


def save_state(state):
    state["seen"] = list(
        dict.fromkeys(state["seen"])
    )[-MAX_SEEN_EVENTS:]

    state["pending"] = state["pending"][
        -MAX_PENDING_EVENTS:
    ]

    state["skipped_ranges"] = state[
        "skipped_ranges"
    ][-100:]

    temp_file = STATE_FILE.with_suffix(".tmp")

    temp_file.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    temp_file.replace(STATE_FILE)


def address_from_topic(topic):
    if not topic or len(topic) < 42:
        return None

    try:
        return Web3.to_checksum_address(
            "0x" + topic[-40:]
        )
    except Exception:
        return None


def address_from_data(data, word_index):
    if (
        not isinstance(data, str)
        or not data.startswith("0x")
    ):
        return None

    raw = data[2:]
    start = word_index * 64
    word = raw[start:start + 64]

    if len(word) != 64:
        return None

    try:
        return Web3.to_checksum_address(
            "0x" + word[-40:]
        )
    except Exception:
        return None


def get_pool_events(start_block, end_block):
    if end_block < start_block:
        return []

    if end_block - start_block + 1 > CHUNK_SIZE:
        raise ValueError(
            "eth_getLogs chunk exceeds CHUNK_SIZE"
        )

    # JSON-RPC quantity values must include 0x.
    from_block_hex = hex(int(start_block))
    to_block_hex = hex(int(end_block))

    if (
        not from_block_hex.startswith("0x")
        or not to_block_hex.startswith("0x")
    ):
        raise ValueError(
            "eth_getLogs block quantity "
            "is missing 0x prefix"
        )

    if not POOL_CREATED_TOPIC.startswith("0x"):
        raise ValueError(
            "PoolCreated topic is missing 0x prefix"
        )

    if any(
        not address.startswith("0x")
        for address in FACTORIES
    ):
        raise ValueError(
            "A factory address is missing 0x prefix"
        )

    # Log the exact filter fields, without exposing RPC secrets.
    log(
        "eth_getLogs filter: "
        f"fromBlock={from_block_hex}, "
        f"toBlock={to_block_hex}, "
        f"factory_count={len(FACTORIES)}, "
        f"topic={POOL_CREATED_TOPIC}"
    )

    result = rpc(
        "eth_getLogs",
        [{
            "fromBlock": from_block_hex,
            "toBlock": to_block_hex,
            "address": FACTORIES,
            "topics": [POOL_CREATED_TOPIC],
        }],
    )

    events = []

    for item in result:
        topics = item.get("topics", [])

        if len(topics) < 3:
            continue

        token0 = address_from_topic(topics[1])
        token1 = address_from_topic(topics[2])

        # V3 PoolCreated data:
        # word 0 = tickSpacing
        # word 1 = pool address
        pool_address = address_from_data(
            item.get("data", ""),
            1,
        )

        if (
            not token0
            or not token1
            or not pool_address
        ):
            log(
                "Warning: ignored malformed "
                "PoolCreated event"
            )
            continue

        tx_hash = item.get(
            "transactionHash",
            "",
        )

        log_index = int(
            item.get("logIndex", "0x0"),
            16,
        )

        block_number = int(
            item.get("blockNumber", "0x0"),
            16,
        )

        if not tx_hash:
            continue

        event_id = (
            f"{tx_hash.lower()}:{log_index}"
        )

        events.append({
            "id": event_id,
            "block": block_number,
            "tx": tx_hash,
            "log_index": log_index,
            "pool": pool_address,
            "token0": token0,
            "token1": token1,
            "first_seen": int(time.time()),
        })

    return events


def add_pending_events(state, events):
    existing = {
        item.get("id")
        for item in state["pending"]
        if isinstance(item, dict)
    }

    seen = set(state["seen"])

    for event in events:
        event_id = event["id"]

        if (
            event_id in existing
            or event_id in seen
        ):
            continue

        state["pending"].append(event)
        existing.add(event_id)

    if len(state["pending"]) > MAX_PENDING_EVENTS:
        log(
            "ERROR: pending queue exceeded its "
            "safety limit. Some newest events "
            "cannot be retained."
        )

        state["pending"] = state["pending"][
            -MAX_PENDING_EVENTS:
        ]


def fetch_market_pairs(tokens):
    """Return (pairs, success). False means data retrieval was incomplete."""
    tokens = list(
        dict.fromkeys(
            token.lower()
            for token in tokens
            if token
        )
    )

    if not tokens:
        return [], True

    all_pairs = []
    all_succeeded = True

    # Try the batch endpoint first.
    for offset in range(0, len(tokens), 30):
        batch = tokens[offset:offset + 30]

        url = (
            "https://api.dexscreener.com/tokens/v1/robinhood/"
            + ",".join(batch)
        )

        success = False

        for attempt in range(3):
            try:
                response = session.get(
                    url,
                    timeout=REQUEST_TIMEOUT,
                )

                if (
                    response.status_code == 429
                    or response.status_code >= 500
                ):
                    time.sleep(
                        min(2 ** attempt, 10)
                    )
                    continue

                response.raise_for_status()
                data = response.json()

                if isinstance(data, list):
                    all_pairs.extend(data)
                    success = True
                    break

                log(
                    "Market API returned an "
                    "unexpected response format"
                )
                break

            except (
                requests.RequestException,
                ValueError,
            ) as exc:
                log(
                    f"Market API batch request failed: {exc}"
                )
                time.sleep(
                    min(2 ** attempt, 10)
                )

        if not success:
            # Fallback to individual token lookups.
            batch_success = True

            for token in batch:
                token_ok = False

                fallback_url = (
                    "https://api.dexscreener.com/"
                    "latest/dex/tokens/"
                    + token
                )

                for attempt in range(2):
                    try:
                        response = session.get(
                            fallback_url,
                            timeout=REQUEST_TIMEOUT,
                        )

                        if (
                            response.status_code == 429
                            or response.status_code >= 500
                        ):
                            time.sleep(
                                min(2 ** attempt, 8)
                            )
                            continue

                        response.raise_for_status()
                        data = response.json()

                        if (
                            isinstance(data, dict)
                            and "pairs" in data
                        ):
                            pairs = data.get("pairs") or []

                            if isinstance(pairs, list):
                                all_pairs.extend(pairs)
                                token_ok = True

                        break

                    except (
                        requests.RequestException,
                        ValueError,
                    ) as exc:
                        log(
                            "Fallback market lookup "
                            f"failed: {exc}"
                        )
                        time.sleep(
                            min(2 ** attempt, 8)
                        )

                if not token_ok:
                    batch_success = False

            if not batch_success:
                all_succeeded = False

    # Deduplicate by chain and pair address.
    unique = {}

    for pair in all_pairs:
        if not isinstance(pair, dict):
            continue

        key = (
            str(pair.get("chainId", "")).lower(),
            str(
                pair.get("pairAddress", "")
            ).lower(),
        )

        unique[key] = pair

    return list(unique.values()), all_succeeded


def evaluate_pair(pair, token_address, now_ms):
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}

    candidate = token_address.lower()

    base_address = str(
        base.get("address", "")
    ).lower()

    quote_address = str(
        quote.get("address", "")
    ).lower()

    if candidate not in (
        base_address,
        quote_address,
    ):
        return None

    liquidity = pair.get("liquidity") or {}
    volume = pair.get("volume") or {}
    txns = pair.get("txns") or {}
    h24 = txns.get("h24") or {}

    try:
        liquidity_usd = float(
            liquidity.get("usd") or 0
        )

        volume_usd = float(
            volume.get("h24") or 0
        )

        buys = int(h24.get("buys") or 0)
        sells = int(h24.get("sells") or 0)

        txn_count = buys + sells

        created_at = int(
            pair.get("pairCreatedAt") or 0
        )

    except (TypeError, ValueError):
        return None

    if liquidity_usd < MIN_LIQUIDITY_USD:
        return None

    if volume_usd < MIN_VOLUME_24H_USD:
        return None

    if txn_count < MIN_TXNS_24H:
        return None

    # Reject pairs with no sell activity or
    # an unusually high volume/liquidity ratio.
    if sells <= 0:
        return None

    if buys / sells < MIN_BUY_SELL_RATIO:
        return None

    if (
        volume_usd / max(liquidity_usd, 1)
        > MAX_VOLUME_LIQUIDITY_RATIO
    ):
        return None

    if created_at <= 0:
        return None

    age_hours = max(
        0,
        (now_ms - created_at) / 3600000,
    )

    if age_hours > MAX_PAIR_AGE_DAYS * 24:
        return None

    price_change = pair.get("priceChange") or {}

    try:
        change_24h = float(
            price_change.get("h24") or 0
        )
    except (TypeError, ValueError):
        change_24h = 0.0

    score = 0
    score += min(
        30,
        liquidity_usd / 300000 * 15,
    )
    score += min(
        25,
        volume_usd / 100000 * 10,
    )
    score += min(
        20,
        txn_count / 20 * 5,
    )
    score += 15 if buys >= sells else 5
    score += 10 if age_hours <= 24 else 5
    score = int(min(100, score))

    return {
        "pair": pair,
        "token": (
            base
            if base_address == candidate
            else quote
        ),
        "liquidity": liquidity_usd,
        "volume": volume_usd,
        "buys": buys,
        "sells": sells,
        "txns": txn_count,
        "age_hours": age_hours,
        "change_24h": change_24h,
        "score": score,
    }


def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        log(
            "Missing GitHub Secret: SERVER_CHAN_KEY"
        )
        return False

    url = (
        f"https://sctapi.ftqq.com/"
        f"{SERVER_CHAN_KEY}.send"
    )

    for attempt in range(3):
        try:
            response = session.post(
                url,
                data={
                    "title": title,
                    "desp": body,
                },
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code != 200:
    log(f"ServerChan HTTP {response.status_code}: {response.text[:500]}")
    return False

result = response.json()
            result = response.json()

            if result.get("code") == 0:
                log(
                    f"WeChat notification sent: {title}"
                )
                return True

            log(
                "ServerChan rejected notification: "
                f"{result}"
            )

        except (
            requests.RequestException,
            ValueError,
        ) as exc:
            log(f"WeChat push failed: {exc}")

        time.sleep(
            min(2 ** attempt, 8)
        )

    return False


def event_tokens(event):
    return [
        event.get("token0"),
        event.get("token1"),
    ]


def format_candidate(event, candidate):
    pair = candidate["pair"]
    token = candidate["token"]

    name = token.get("name") or "Unknown"
    symbol = token.get("symbol") or "?"

    token_address = token.get("address") or ""
    pair_address = pair.get("pairAddress") or ""
    dex = pair.get("dexId") or "Unknown DEX"
    url = pair.get("url") or ""

    title = (
        f"Robinhood候选币：{symbol} | "
        f"评分{candidate['score']}"
    )

    body = "\n".join([
        f"代币：{name} ({symbol})",
        f"合约：{token_address}",
        f"交易池：{pair_address}",
        f"DEX：{dex}",
        f"流动性：${candidate['liquidity']:,.0f}",
        f"24小时成交量：${candidate['volume']:,.0f}",
        (
            f"24小时买/卖："
            f"{candidate['buys']}/"
            f"{candidate['sells']}"
        ),
        f"池龄：{candidate['age_hours']:.1f} 小时",
        (
            f"24小时涨跌："
            f"{candidate['change_24h']:.2f}%"
        ),
        f"评分：{candidate['score']}/100",
        f"区块：{event['block']}",
        f"交易：{event['tx']}",
        f"行情链接：{url}",
        "",
        (
            "提示：这是自动筛选候选，不代表安全或建议买入。"
            "请先核实合约、持币集中度、LP锁定及买卖税。"
        ),
    ])

    return title, body


def main():
    log("Starting Robinhood Chain scanner")
    log(f"Chain ID expected: {CHAIN_ID}")

    chain_id = int(
        rpc("eth_chainId"),
        16,
    )

    if chain_id != CHAIN_ID:
        raise RuntimeError(
            f"Wrong chain ID: expected {CHAIN_ID}, "
            f"got {chain_id}"
        )

    latest_block = int(
        rpc("eth_blockNumber"),
        16,
    )

    state = load_state()
    last_block = state["last_block"]

    if last_block is None:
        # First run starts near the chain tip.
        start_block = max(
            0,
            latest_block - MAX_BLOCKS_PER_RUN,
        )

        log(
            f"First run: initial scan starts at block "
            f"{start_block}; latest block is {latest_block}"
        )

    else:
        last_block = int(last_block)

        if last_block > latest_block:
            log(
                "Saved block is ahead of RPC tip; "
                "resetting scan cursor near tip"
            )

            last_block = max(
                0,
                latest_block - MAX_BLOCKS_PER_RUN,
            )

        lag = latest_block - last_block

        if lag > MAX_LAG_BLOCKS:
            new_start = max(
                0,
                latest_block - MAX_BLOCKS_PER_RUN,
            )

            skipped_from = last_block + 1
            skipped_to = new_start

            if skipped_to >= skipped_from:
                skipped = {
                    "from": skipped_from,
                    "to": skipped_to,
                    "recorded_at": int(time.time()),
                    "reason": (
                        "historical backlog exceeded "
                        "safety threshold"
                    ),
                }

                state["skipped_ranges"].append(skipped)

                log(
                    f"BACKLOG SKIP: blocks "
                    f"{skipped_from}-{skipped_to}. "
                    "Pools in this range may be missed."
                )

            last_block = new_start

        start_block = last_block + 1

    if start_block > latest_block:
        log(
            "Already at the latest block; "
            "processing pending events only"
        )

    end_block = min(
        latest_block,
        start_block + MAX_BLOCKS_PER_RUN - 1,
    )

    # Persist events and cursor after each successful query.
    cursor = start_block

    while cursor <= end_block:
        chunk_end = min(
            end_block,
            cursor + CHUNK_SIZE - 1,
        )

        events = get_pool_events(
            cursor,
            chunk_end,
        )

        add_pending_events(
            state,
            events,
        )

        state["last_block"] = chunk_end
        save_state(state)

        log(
            f"Scanned blocks {cursor}-{chunk_end}; "
            f"events={len(events)}; "
            f"pending={len(state['pending'])}"
        )

        cursor = chunk_end + 1

    if not state["pending"]:
        save_state(state)

        log(
            f"Scan complete. Latest block: "
            f"{latest_block}; no pending events."
        )
        return

    # Expire events that have no market listing after 24 hours.
    now_seconds = int(time.time())
    active_pending = []
    expired_count = 0

    for event in state["pending"]:
        first_seen = int(
            event.get("first_seen") or now_seconds
        )

        if (
            now_seconds - first_seen
            > PENDING_MAX_AGE_SECONDS
        ):
            state["seen"].append(
                event.get("id", "")
            )
            expired_count += 1
        else:
            active_pending.append(event)

    state["pending"] = active_pending

    if expired_count:
        log(
            f"Expired {expired_count} pending events "
            "without timely market data"
        )

    if not state["pending"]:
        save_state(state)
        log("No pending events left to process")
        return

    tokens = []

    for event in state["pending"]:
        tokens.extend(
            event_tokens(event)
        )

    pairs, api_success = fetch_market_pairs(tokens)

    if not api_success:
        # Keep events for another run.
        save_state(state)

        log(
            "Market API was incomplete. Pending events "
            "retained for retry; no event was discarded "
            "because of API failure."
        )
        return

    now_ms = int(time.time() * 1000)
    pairs_by_token = {}

    for pair in pairs:
        for token_field in (
            "baseToken",
            "quoteToken",
        ):
            token_info = pair.get(token_field) or {}

            address = str(
                token_info.get("address") or ""
            ).lower()

            if address:
                pairs_by_token.setdefault(
                    address,
                    [],
                ).append(pair)

    remaining = []
    seen_now = set(state["seen"])
    pushed = 0
    checked = 0

    for event in state["pending"]:
        if pushed >= MAX_PUSHES:
            remaining.append(event)
            continue

        event_id = event.get("id")

        if not event_id or event_id in seen_now:
            continue

        event_token_list = [
            token.lower()
            for token in event_tokens(event)
            if token
        ]

        # Retry later if neither token has market data.
        has_market_data = any(
            token in pairs_by_token
            for token in event_token_list
        )

        if not has_market_data:
            remaining.append(event)
            continue

        checked += 1
        candidates = []

        for token in event_token_list:
            for pair in pairs_by_token.get(token, []):
                candidate = evaluate_pair(
                    pair,
                    token,
                    now_ms,
                )

                if candidate:
                    candidates.append(candidate)

        if candidates:
            candidates.sort(
                key=lambda item: (
                    item["score"],
                    item["liquidity"],
                ),
                reverse=True,
            )

            best = candidates[0]

            title, body = format_candidate(
                event,
                best,
            )

            if send_wechat(title, body):
                pushed += 1
                seen_now.add(event_id)
                state["seen"].append(event_id)

            else:
                # Failed push: retry next run.
                remaining.append(event)

        else:
            # Market data exists, but filters were not passed.
            seen_now.add(event_id)
            state["seen"].append(event_id)

    state["pending"] = remaining
    save_state(state)

    log(
        f"Run complete | latest={latest_block} | "
        f"last_scanned={state['last_block']} | "
        f"checked={checked} | pushed={pushed} | "
        f"pending={len(state['pending'])}"
    )

    if state["last_block"] < latest_block:
        log(
            f"More blocks remain. Next run will continue "
            f"after {state['last_block']}."
        )


if __name__ == "__main__":
    try:
        if os.getenv("TEST_PUSH") == "1":
            log("Starting WeChat push test")

            if send_wechat(
                "Robinhood 扫链机器人测试",
                "微信推送测试成功！\n\n"
                "说明：Server酱通知接口已成功发送测试消息。\n"
                "本消息不是买币信号，没有执行任何交易。"
            ):
                log("TEST PUSH SUCCESS")
                sys.exit(0)
            else:
                log("TEST PUSH FAILED")
                sys.exit(1)

        main()

    except Exception as exc:
        log(f"FATAL: {exc}")
        sys.exit(1)
