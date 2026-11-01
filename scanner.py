
import os
import sys
import json
import time
import requests

from pathlib import Path
from datetime import datetime, timezone
from web3 import Web3


# Robinhood Chain scanner
# Read only. Never buys or sells tokens.

RPC_URL = os.getenv(
    "ALCHEMY_RPC_URL", ""
).strip()

SEND_KEY = os.getenv(
    "SERVER_CHAN_KEY", ""
).strip()

TEST_PUSH = (
    os.getenv("TEST_PUSH", "0") == "1"
)

CHAIN_ID = 4663

FACTORY = (
    "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA"
)

POOL_TOPIC = Web3.to_hex(
    Web3.keccak(
        text="PoolCreated(address,address,uint24,int24,address)"
    )
)

STATE_FILE = Path("state.json")

# Changing this version starts fresh near the chain tip.
SCANNER_VERSION = "live-v2"

CHUNK_SIZE = 10
MAX_BLOCKS_PER_RUN = 300
MAX_BACKLOG = 600
CONFIRMATIONS = 3

MAX_PENDING = 300
MAX_PUSHES = 8

MIN_LIQUIDITY = 300000
MIN_VOLUME = 100000
MIN_TXNS = 20
MAX_AGE_HOURS = 72

TIMEOUT = 25

session = requests.Session()


def log(*args):
    stamp = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

    print(stamp, *args, flush=True)


def safe_text(value):
    result = str(value)

    for secret in (RPC_URL, SEND_KEY):
        if secret:
            result = result.replace(
                secret, "[REDACTED]"
            )

    return result


def rpc(method, params=None):
    if not RPC_URL:
        raise RuntimeError(
            "Missing ALCHEMY_RPC_URL"
        )

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or [],
    }

    for attempt in range(4):
        try:
            response = session.post(
                RPC_URL,
                json=payload,
                timeout=TIMEOUT,
            )

            if response.status_code != 200:
                log(
                    "RPC HTTP",
                    response.status_code,
                    "method=" + method,
                    "detail=" + safe_text(
                        response.text[:250]
                    ),
                )

                response.raise_for_status()

            data = response.json()

            if "error" in data:
                raise RuntimeError(
                    safe_text(data["error"])
                )

            if "result" not in data:
                raise RuntimeError(
                    "RPC missing result"
                )

            return data["result"]

        except Exception as exc:
            log(
                "RPC retry",
                method,
                type(exc).__name__,
                safe_text(exc)[:200],
            )

            if attempt == 3:
                raise

            time.sleep(2 ** attempt)


def push(title, message):
    if not SEND_KEY:
        log("Missing SERVER_CHAN_KEY")
        return False

    url = (
        "https://sctapi.ftqq.com/"
        + SEND_KEY
        + ".send"
    )

    for attempt in range(3):
        try:
            response = session.post(
                url,
                data={
                    "title": title,
                    "desp": message,
                },
                timeout=TIMEOUT,
            )

            if response.status_code != 200:
                log(
                    "WeChat HTTP",
                    response.status_code,
                )

                if response.status_code in (
                    400, 401, 403
                ):
                    return False

                time.sleep(2 ** attempt)
                continue

            data = response.json()

            if data.get("code") == 0:
                log("WeChat push success")
                return True

            log(
                "WeChat API code:",
                data.get("code"),
            )

            return False

        except Exception as exc:
            log(
                "WeChat error",
                type(exc).__name__,
            )

            time.sleep(2 ** attempt)

    return False


def default_state():
    return {
        "version": SCANNER_VERSION,
        "last_block": None,
        "pending": [],
        "seen": [],
        "last_report_day": None,
    }


def load_state():
    if not STATE_FILE.exists():
        return default_state()

    data = json.loads(
        STATE_FILE.read_text(
            encoding="utf-8"
        )
    )

    if not isinstance(data, dict):
        raise RuntimeError(
            "Invalid state file"
        )

    if data.get("version") != SCANNER_VERSION:
        log(
            "Scanner upgraded:",
            "old historical backlog reset"
        )

        return default_state()

    for key, value in default_state().items():
        data.setdefault(key, value)

    if not isinstance(data["pending"], list):
        data["pending"] = []

    if not isinstance(data["seen"], list):
        data["seen"] = []

    return data


def save_state(state):
    temp = Path("state.tmp")

    temp.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    temp.replace(STATE_FILE)


def decode_address(value):
    return Web3.to_checksum_address(
        "0x" + value[-40:]
    )


def scan_events(start, end):
    raw_logs = rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "address": FACTORY,
            "topics": [POOL_TOPIC],
        }],
    )

    events = []

    for item in raw_logs:
        try:
            topics = item["topics"]
            data = item["data"].removeprefix("0x")

            if len(topics) != 4:
                continue

            if len(data) < 128:
                continue

            pool = decode_address(
                data[64:128]
            )

            tx = item["transactionHash"]

            event_id = (
                tx.lower()
                + ":"
                + str(
                    int(item["logIndex"], 16)
                )
            )

            events.append({
                "id": event_id,
                "pool": pool,
                "token0": decode_address(
                    topics[1]
                ),
                "token1": decode_address(
                    topics[2]
                ),
                "block": int(
                    item["blockNumber"], 16
                ),
                "first_seen": int(time.time()),
            })

        except Exception as exc:
            log(
                "Event decode error",
                type(exc).__name__,
            )

    return events


def fetch_pair(pool):
    url = (
        "https://api.dexscreener.com/"
        "latest/dex/pairs/robinhood/"
        + pool
    )

    try:
        response = session.get(
            url,
            timeout=TIMEOUT,
        )

        log(
            "Market HTTP",
            response.status_code,
            "pool=" + pool[:12],
        )

        if response.status_code != 200:
            return None, "api_error"

        data = response.json()

        pairs = data.get("pairs") or []

        for pair in pairs:
            address = str(
                pair.get(
                    "pairAddress", ""
                )
            ).lower()

            if address == pool.lower():
                return pair, "found"

        return None, "not_listed"

    except Exception as exc:
        log(
            "Market API error",
            type(exc).__name__,
        )

        return None, "api_error"


def evaluate(pair):
    try:
        liquidity = float(
            (pair.get("liquidity") or {})
            .get("usd") or 0
        )

        volume = float(
            (pair.get("volume") or {})
            .get("h24") or 0
        )

        txns = (
            (pair.get("txns") or {})
            .get("h24") or {}
        )

        buys = int(
            txns.get("buys") or 0
        )

        sells = int(
            txns.get("sells") or 0
        )

        created = int(
            pair.get("pairCreatedAt") or 0
        )

        if created <= 0:
            return None

        age = (
            time.time() * 1000 - created
        ) / 3600000

        if liquidity < MIN_LIQUIDITY:
            return None

        if volume < MIN_VOLUME:
            return None

        if buys + sells < MIN_TXNS:
            return None

        if sells <= 0:
            return None

        if buys / sells < 0.8:
            return None

        if volume / max(
            liquidity, 1
        ) > 5:
            return None

        if not (
            0 <= age <= MAX_AGE_HOURS
        ):
            return None

        return {
            "liquidity": liquidity,
            "volume": volume,
            "buys": buys,
            "sells": sells,
            "age": age,
        }

    except (
        ValueError,
        TypeError,
        ZeroDivisionError,
    ):
        return None


def format_candidate(event, pair, info):
    token = pair.get("baseToken") or {}

    symbol = str(
        token.get("symbol", "?")
    )

    title = (
        "Robinhood候选币：" + symbol
    )

    body = "\n".join([
        "代币：" + str(
            token.get("name", "?")
        ),
        "合约：" + str(
            token.get("address", "")
        ),
        "交易池：" + event["pool"],
        "流动性：$"
        + format(info["liquidity"], ",.0f"),
        "24小时成交量：$"
        + format(info["volume"], ",.0f"),
        "买入笔数：" + str(info["buys"]),
        "卖出笔数：" + str(info["sells"]),
        "池龄："
        + format(info["age"], ".1f")
        + "小时",
        "区块：" + str(event["block"]),
        "行情：" + str(
            pair.get("url", "")
        ),
        "",
        "仅供观察，不是买入建议。",
        "尚未核实持仓集中度、"
        "合约权限和LP安全。",
    ])

    return title, body


def main():
    log("Starting Robinhood live scanner")

    if TEST_PUSH:
        if not push(
            "Robinhood扫描器测试",
            "测试成功，不是买币信号。",
        ):
            raise RuntimeError(
                "TEST PUSH FAILED"
            )

        return

    chain = int(
        rpc("eth_chainId"), 16
    )

    if chain != CHAIN_ID:
        raise RuntimeError(
            "Wrong chain: " + str(chain)
        )

    latest = int(
        rpc("eth_blockNumber"), 16
    )

    target = max(
        0, latest - CONFIRMATIONS
    )

    code = rpc(
        "eth_getCode",
        [FACTORY, "latest"],
    )

    if code in ("0x", "0x0", None):
        raise RuntimeError(
            "Factory bytecode missing"
        )

    log(
        "Connected",
        "latest=", latest,
        "target=", target,
    )

    state = load_state()

    last = state["last_block"]

    if last is None:
        start = max(
            0,
            target - MAX_BLOCKS_PER_RUN + 1,
        )

        log(
            "LIVE MODE INITIALIZED",
            "start=", start,
        )

    else:
        start = int(last) + 1

        backlog = target - int(last)

        if backlog > MAX_BACKLOG:
            old_start = start

            start = max(
                0,
                target - MAX_BLOCKS_PER_RUN + 1,
            )

            log(
                "BACKLOG SKIPPED",
                old_start,
                "to",
                start - 1,
            )

    found = 0

    seen = set(state["seen"])
    pending = state["pending"]

    pending_ids = {
        e.get("id")
        for e in pending
        if isinstance(e, dict)
    }

    if start <= target:
        end = min(
            target,
            start + MAX_BLOCKS_PER_RUN - 1,
        )

        for block in range(
            start,
            end + 1,
            CHUNK_SIZE,
        ):
            chunk_end = min(
                block + CHUNK_SIZE - 1,
                end,
            )

            events = scan_events(
                block,
                chunk_end,
            )

            found += len(events)

            for event in events:
                if (
                    event["id"] not in seen
                    and event["id"] not in pending_ids
                ):
                    pending.append(event)

                    pending_ids.add(
                        event["id"]
                    )

            state["last_block"] = chunk_end
            state["pending"] = pending

            save_state(state)

            time.sleep(0.3)

        log(
            "Scan complete",
            "start=", start,
            "end=", end,
            "events=", found,
            "lag=", target - end,
        )

    else:
        log("No new confirmed blocks")

    pushed = 0
    rejected = 0
    not_listed = 0
    api_errors = 0

    remaining = []

    now = time.time()

    for event in pending:
        if pushed >= MAX_PUSHES:
            remaining.append(event)
            continue

        first_seen = float(
            event.get("first_seen") or now
        )

        age_seconds = now - first_seen

        if age_seconds > 72 * 3600:
            seen.add(event["id"])
            continue

        pair, status = fetch_pair(
            event["pool"]
        )

        if status == "api_error":
            api_errors += 1
            remaining.append(event)
            continue

        if status == "not_listed":
            not_listed += 1
            remaining.append(event)
            continue

        info = evaluate(pair)

        if info is None:
            rejected += 1
            remaining.append(event)
            continue

        title, body = format_candidate(
            event,
            pair,
            info,
        )

        if push(title, body):
            pushed += 1
            seen.add(event["id"])
        else:
            remaining.append(event)

        time.sleep(0.25)

    state["pending"] = remaining[:MAX_PENDING]
    state["seen"] = list(seen)[-10000:]

    log(
        "Summary",
        "pushed=", pushed,
        "rejected=", rejected,
        "not_listed=", not_listed,
        "api_errors=", api_errors,
        "pending=", len(state["pending"]),
    )

    today = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    if state.get("last_report_day") != today:
        report = "\n".join([
            "Robinhood实时扫描运行报告",
            "链ID：4663",
            "最新区块：" + str(latest),
            "已扫描至：" + str(
                state.get("last_block")
            ),
            "本次发现事件：" + str(found),
            "待处理池：" + str(
                len(state["pending"])
            ),
            "行情未收录：" + str(not_listed),
            "行情接口错误：" + str(api_errors),
            "候选币推送：" + str(pushed),
            "",
            "当前仅监听一个V3 Factory。",
            "不代表覆盖全链全部新币。",
        ])

        if push(
            "Robinhood每日运行报告",
            report,
        ):
            state["last_report_day"] = today

    save_state(state)

    log("Run finished successfully")


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        log(
            "FATAL",
            type(exc).__name__,
            safe_text(exc)[:400],
        )

        sys.exit(1)
