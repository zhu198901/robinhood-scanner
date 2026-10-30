
import os
import sys
import json
import time
import requests

from pathlib import Path
from datetime import datetime, timezone
from web3 import Web3


# Robinhood Chain Scanner
# Read-only: never executes trades

RPC_URL = os.getenv(
    "ALCHEMY_RPC_URL", ""
).strip()

SEND_KEY = os.getenv(
    "SERVER_CHAN_KEY", ""
).strip()

TEST_PUSH = (
    os.getenv("TEST_PUSH", "0") == "1"
)

STATE_FILE = Path("state.json")

CHAIN_ID = 4663

FACTORY_V3 = (
    "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA"
)

POOL_TOPIC = Web3.to_hex(
    Web3.keccak(
        text=(
            "PoolCreated("
            "address,address,uint24,int24,address)"
        )
    )
)

CHUNK_SIZE = 10
MAX_BLOCKS_PER_RUN = 3000

MIN_LIQUIDITY = 300000
MIN_VOLUME = 100000
MIN_TXNS = 20
MAX_AGE_HOURS = 72

MAX_PUSHES = 8
TIMEOUT = 25

session = requests.Session()


def log(*args):
    now = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

    print(now, *args, flush=True)


def safe_text(value):
    value = str(value)

    for secret in (RPC_URL, SEND_KEY):
        if secret:
            value = value.replace(
                secret, "[REDACTED]"
            )

    return value


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
                detail = safe_text(
                    response.text[:500]
                )

                log(
                    "RPC HTTP error",
                    response.status_code,
                    "method=" + method,
                    "details=" + detail,
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
                "RPC failed:",
                method,
                type(exc).__name__,
                safe_text(exc)[:300],
            )

            if attempt == 3:
                raise

            time.sleep(
                min(2 ** attempt, 8)
            )


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
                    "ServerChan HTTP",
                    response.status_code,
                    safe_text(
                        response.text[:300]
                    ),
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
                "ServerChan error:",
                safe_text(data)[:300],
            )

            return False

        except Exception as exc:
            log(
                "WeChat error:",
                type(exc).__name__,
            )

            time.sleep(2 ** attempt)

    return False


def load_state():
    default = {
        "last_block": None,
        "pending": [],
        "seen": [],
        "last_report_day": None,
    }

    if not STATE_FILE.exists():
        return default

    data = json.loads(
        STATE_FILE.read_text(
            encoding="utf-8"
        )
    )

    if not isinstance(data, dict):
        raise RuntimeError(
            "Invalid state.json"
        )

    for key, value in default.items():
        data.setdefault(key, value)

    if not isinstance(
        data["pending"], list
    ):
        data["pending"] = []

    if not isinstance(
        data["seen"], list
    ):
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
    result = rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "address": FACTORY_V3,
            "topics": [POOL_TOPIC],
        }],
    )

    events = []

    for item in result:
        try:
            topics = item["topics"]

            if len(topics) != 4:
                continue

            raw = item["data"].removeprefix(
                "0x"
            )

            if len(raw) < 128:
                continue

            pool = decode_address(
                raw[64:128]
            )

            tx = item[
                "transactionHash"
            ]

            index = int(
                item["logIndex"], 16
            )

            block = int(
                item["blockNumber"], 16
            )

            events.append({
                "id": (
                    tx.lower()
                    + ":"
                    + str(index)
                ),
                "pool": pool,
                "token0": decode_address(
                    topics[1]
                ),
                "token1": decode_address(
                    topics[2]
                ),
                "block": block,
                "first_seen": int(
                    time.time()
                ),
            })

        except Exception as exc:
            log(
                "Event decode error:",
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

        response.raise_for_status()

        data = response.json()

        pairs = data.get("pairs") or []

        for pair in pairs:
            if (
                str(
                    pair.get(
                        "pairAddress", ""
                    )
                ).lower()
                == pool.lower()
            ):
                return pair

    except Exception as exc:
        log(
            "Market API error:",
            type(exc).__name__,
        )

    return None


def evaluate(pair):
    try:
        liquidity = float(
            (
                pair.get("liquidity")
                or {}
            ).get("usd") or 0
        )

        volume = float(
            (
                pair.get("volume")
                or {}
            ).get("h24") or 0
        )

        txns = (
            (
                pair.get("txns")
                or {}
            ).get("h24")
            or {}
        )

        buys = int(
            txns.get("buys") or 0
        )

        sells = int(
            txns.get("sells") or 0
        )

        created = int(
            pair.get(
                "pairCreatedAt"
            ) or 0
        )

        if not created:
            return None

        age = (
            time.time() * 1000
            - created
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
        TypeError,
        ValueError,
        ZeroDivisionError,
    ):
        return None


def format_message(event, pair, info):
    token = (
        pair.get("baseToken")
        or {}
    )

    symbol = token.get(
        "symbol", "?"
    )

    title = (
        "Robinhood候选币："
        + str(symbol)
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
        + format(
            info["liquidity"], ",.0f"
        ),
        "24h成交量：$"
        + format(
            info["volume"], ",.0f"
        ),
        "买入笔数："
        + str(info["buys"]),
        "卖出笔数："
        + str(info["sells"]),
        "池龄："
        + format(
            info["age"], ".1f"
        )
        + "小时",
        "区块："
        + str(event["block"]),
        "行情："
        + str(
            pair.get("url", "")
        ),
        "",
        "仅供候选观察，"
        "不代表安全或买入建议。",
        "请核查合约权限、"
        "持币集中度和LP风险。",
    ])

    return title, body


def main():
    log(
        "Starting Robinhood Scanner"
    )

    if TEST_PUSH:
        success = push(
            "Robinhood扫描器测试",
            "微信测试成功。"
            "本消息不是买币信号。",
        )

        if not success:
            raise RuntimeError(
                "TEST PUSH FAILED"
            )

        log("TEST PUSH SUCCESS")
        return

    chain = int(
        rpc("eth_chainId"), 16
    )

    if chain != CHAIN_ID:
        raise RuntimeError(
            "Wrong Chain ID: "
            + str(chain)
        )

    latest = int(
        rpc("eth_blockNumber"),
        16,
    )

    log(
        "Connected, latest block:",
        latest,
    )

    code = rpc(
        "eth_getCode",
        [
            FACTORY_V3,
            "latest",
        ],
    )

    if code in (
        "0x", "0x0", None
    ):
        raise RuntimeError(
            "Factory contract not found"
        )

    log("Factory bytecode exists")

    state = load_state()

    last = state["last_block"]

    if last is None:
        start = max(
            0,
            latest - 299,
        )
    else:
        start = int(last) + 1

    if start > latest:
        log("No new blocks")

    else:
        end = min(
            latest,
            start
            + MAX_BLOCKS_PER_RUN
            - 1,
        )

        found = 0

        pending = state["pending"]

        seen = set(
            state["seen"]
        )

        pending_ids = {
            e.get("id")
            for e in pending
            if isinstance(e, dict)
        }

        for block in range(
            start,
            end + 1,
            CHUNK_SIZE,
        ):
            chunk_end = min(
                block
                + CHUNK_SIZE
                - 1,
                end,
            )

            events = scan_events(
                block,
                chunk_end,
            )

            for event in events:
                if (
                    event["id"]
                    not in seen
                    and event["id"]
                    not in pending_ids
                ):
                    pending.append(
                        event
                    )

                    pending_ids.add(
                        event["id"]
                    )

            found += len(events)

            state["last_block"] = (
                chunk_end
            )

            state["pending"] = (
                pending
            )

            save_state(state)

            if (
                (block - start)
                // CHUNK_SIZE
            ) % 20 == 0:
                log(
                    "Scanned",
                    block,
                    "-",
                    chunk_end,
                    "events=",
                    len(events),
                )

            time.sleep(0.15)

        log(
            "Scan complete:",
            start,
            "-",
            end,
            "events=",
            found,
            "lag=",
            latest - end,
        )

    seen = set(
        state["seen"]
    )

    remaining = []
    pushed = 0
    rejected = 0
    unavailable = 0

    for event in state["pending"]:
        if pushed >= MAX_PUSHES:
            remaining.append(event)
            continue

        pair = fetch_pair(
            event["pool"]
        )

        if pair is None:
            unavailable += 1

            if (
                time.time()
                - event.get(
                    "first_seen", 0
                )
                < 72 * 3600
            ):
                remaining.append(
                    event
                )
            else:
                seen.add(
                    event["id"]
                )

            continue

        info = evaluate(pair)

        if info is None:
            rejected += 1

            if (
                time.time()
                - event.get(
                    "first_seen", 0
                )
                < 24 * 3600
            ):
                remaining.append(
                    event
                )
            else:
                seen.add(
                    event["id"]
                )

            continue

        title, body = (
            format_message(
                event,
                pair,
                info,
            )
        )

        if push(title, body):
            pushed += 1
            seen.add(
                event["id"]
            )
        else:
            remaining.append(
                event
            )

    state["pending"] = (
        remaining[-3000:]
    )

    state["seen"] = (
        list(seen)[-10000:]
    )

    log(
        "Summary:",
        "pushed=", pushed,
        "rejected=", rejected,
        "unavailable=", unavailable,
        "pending=",
        len(remaining),
    )

    today = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    if (
        state.get(
            "last_report_day"
        )
        != today
    ):
        report = "\n".join([
            "Robinhood扫描器运行报告",
            "Chain ID：4663",
            "最新区块："
            + str(latest),
            "已扫描至："
            + str(
                state.get(
                    "last_block"
                )
            ),
            "待处理池："
            + str(
                len(remaining)
            ),
            "本次推送："
            + str(pushed),
            "",
            "当前仅监控一个"
            "Uniswap V3 Factory。",
            "无候选币不代表程序故障。",
        ])

        if push(
            "Robinhood每日运行报告",
            report,
        ):
            state[
                "last_report_day"
            ] = today

    save_state(state)

    log("Run finished successfully")


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        log(
            "FATAL:",
            type(exc).__name__,
            safe_text(exc)[:500],
        )

        sys.exit(1)
