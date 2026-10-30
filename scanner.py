
import os
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timezone
from web3 import Web3

RPC_URL = os.getenv("ALCHEMY_RPC_URL", "").strip()
SEND_KEY = os.getenv("SERVER_CHAN_KEY", "").strip()
TEST_PUSH = os.getenv("TEST_PUSH", "0") == "1"

STATE = Path("state.json")
CHAIN_ID = 4663

FACTORY_V3 = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"

TOPIC = Web3.keccak(
    text="PoolCreated(address,address,uint24,int24,address)"
).to_0x_hex()

SESSION = requests.Session()


def log(*args):
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    print(timestamp, *args, flush=True)


def rpc(method, params=None):
    if not RPC_URL:
        raise RuntimeError("ALCHEMY_RPC_URL missing")

    for attempt in range(4):
        try:
            response = SESSION.post(
                RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params or [],
                },
                timeout=25,
            )
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                raise RuntimeError(str(data["error"]))

            return data["result"]

        except Exception as exc:
            log("RPC failed", type(exc).__name__)

            if attempt == 3:
                raise

            time.sleep(2 ** attempt)


def push(title, message):
    if not SEND_KEY:
        log("SERVER_CHAN_KEY missing")
        return False

    try:
        response = SESSION.post(
            "https://sctapi.ftqq.com/" + SEND_KEY + ".send",
            data={
                "title": title,
                "desp": message,
            },
            timeout=25,
        )

        data = response.json()
        success = response.ok and data.get("code") == 0

        log(
            "WeChat push:",
            "success" if success else
            "failed, code=" + str(data.get("code")),
        )

        return success

    except Exception as exc:
        log("WeChat push failed:", type(exc).__name__)
        return False


def load():
    if STATE.exists():
        data = json.loads(
            STATE.read_text(encoding="utf-8")
        )

        if isinstance(data, dict):
            return data

    return {}


def save(state):
    temp = STATE.with_suffix(".tmp")

    temp.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    temp.replace(STATE)


def word_addr(word):
    return Web3.to_checksum_address(
        "0x" + word[-40:]
    )


def events_for(start, end):
    logs = rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "address": FACTORY_V3,
            "topics": [TOPIC],
        }],
    )

    events = []

    for item in logs:
        try:
            topics = item["topics"]
            raw = item["data"].removeprefix("0x")

            if len(topics) != 4 or len(raw) < 128:
                continue

            pool = word_addr(raw[64:128])

            events.append({
                "id": (
                    item["transactionHash"].lower()
                    + ":"
                    + str(int(item["logIndex"], 16))
                ),
                "pool": pool,
                "token0": word_addr(topics[1]),
                "token1": word_addr(topics[2]),
                "block": int(
                    item["blockNumber"], 16
                ),
                "first_seen": int(time.time()),
            })

        except (KeyError, ValueError, TypeError) as exc:
            log("Invalid event", type(exc).__name__)

    return events


def market(pool):
    try:
        response = SESSION.get(
            "https://api.dexscreener.com/latest/dex/pairs/robinhood/"
            + pool,
            timeout=20,
        )

        response.raise_for_status()

        pairs = response.json().get("pairs") or []

        return next(
            (
                pair for pair in pairs
                if str(
                    pair.get("pairAddress", "")
                ).lower() == pool.lower()
            ),
            None,
        )

    except Exception as exc:
        log(
            "Market lookup unavailable",
            type(exc).__name__,
        )
        return None


def qualifies(pair):
    try:
        liquidity = float(
            (pair.get("liquidity") or {}).get("usd")
            or 0
        )

        volume = float(
            (pair.get("volume") or {}).get("h24")
            or 0
        )

        txns = (
            (pair.get("txns") or {}).get("h24")
            or {}
        )

        buys = int(txns.get("buys") or 0)
        sells = int(txns.get("sells") or 0)

        created = int(
            pair.get("pairCreatedAt") or 0
        )

        age = (
            (time.time() * 1000 - created) / 3600000
            if created else 999999
        )

        qualified = (
            liquidity >= 300000
            and volume >= 100000
            and buys + sells >= 20
            and sells > 0
            and buys / sells >= 0.8
            and volume / max(liquidity, 1) <= 5
            and 0 <= age <= 72
        )

        return qualified, liquidity, volume, age

    except (ValueError, TypeError, ZeroDivisionError):
        return False, 0, 0, 0


def main():
    if TEST_PUSH:
        if not push(
            "Robinhood扫描器测试",
            "这是一条测试消息，不是买币信号。",
        ):
            raise RuntimeError("Test push failed")
        return

    chain = int(rpc("eth_chainId"), 16)

    if chain != CHAIN_ID:
        raise RuntimeError(
            "Wrong chain ID: " + str(chain)
        )

    code = rpc(
        "eth_getCode",
        [FACTORY_V3, "latest"],
    )

    if code in ("0x", "0x0", None):
        raise RuntimeError(
            "V3 factory has no contract bytecode"
        )

    latest = int(
        rpc("eth_blockNumber"), 16
    )

    state = load()
    last = state.get("last_block")

    start = (
        max(0, latest - 299)
        if last is None
        else int(last) + 1
    )

    if start > latest:
        log("No new blocks")

    else:
        end = min(
            latest,
            start + 2999,
        )

        found = 0
        pending = state.get("pending", [])
        seen = set(state.get("seen", []))

        pending_ids = {
            event["id"]
            for event in pending
        }

        for block in range(
            start, end + 1, 50
        ):
            chunk_end = min(
                block + 49, end
            )

            events = events_for(
                block, chunk_end
            )

            for event in events:
                if (
                    event["id"] not in seen
                    and event["id"] not in pending_ids
                ):
                    pending.append(event)
                    pending_ids.add(event["id"])

            found += len(events)

            state["last_block"] = chunk_end
            state["pending"] = pending
            save(state)

        log(
            "Scanned",
            start,
            "to",
            end,
            "found events",
            found,
            "lag",
            latest - end,
        )

    remaining = []
    pushed = 0
    rejected = 0
    unavailable = 0

    for event in state.get("pending", []):
        if pushed >= 8:
            remaining.append(event)
            continue

        pair = market(event["pool"])

        if pair is None:
            if (
                time.time()
                - event.get("first_seen", 0)
                < 72 * 3600
            ):
                remaining.append(event)
            else:
                seen.add(event["id"])

            unavailable += 1
            continue

        ok, liquidity, volume, age = qualifies(
            pair
        )

        if not ok:
            if (
                time.time()
                - event.get("first_seen", 0)
                < 24 * 3600
            ):
                remaining.append(event)
            else:
                seen.add(event["id"])

            rejected += 1
            continue

        token = pair.get("baseToken") or {}

        title = (
            "Robinhood候选池："
            + str(token.get("symbol", "?"))
        )

        body = (
            "代币："
            + str(token.get("name", "?"))
            + "\n合约："
            + str(token.get("address", ""))
            + "\n交易池："
            + event["pool"]
            + "\n流动性：$"
            + format(liquidity, ",.0f")
            + "\n24h成交量：$"
            + format(volume, ",.0f")
            + "\n池龄："
            + format(age, ".1f")
            + "小时\n行情："
            + str(pair.get("url", ""))
            + "\n仅为候选，未完成合约安全审计。"
        )

        if push(title, body):
            seen.add(event["id"])
            pushed += 1
        else:
            remaining.append(event)

    state["pending"] = remaining[:3000]
    state["seen"] = list(seen)[-10000:]

    log(
        "Summary: pushed",
        pushed,
        "rejected",
        rejected,
        "market_unavailable",
        unavailable,
        "pending",
        len(remaining),
    )

    today = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    if state.get("last_report_day") != today:
        report = (
            "扫描正常。链ID：4663"
            + "\n最新区块："
            + str(latest)
            + "\n已扫描至："
            + str(state.get("last_block"))
            + "\n待处理池："
            + str(len(remaining))
            + "\n本次候选通知："
            + str(pushed)
            + "\n注意：仅监控Uniswap V3 Factory，"
            + "不代表全链覆盖。"
        )

        if push(
            "Robinhood扫描器每日运行报告",
            report,
        ):
            state["last_report_day"] = today

    save(state)


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        message = str(exc)[:200]

        if RPC_URL:
            message = message.replace(
                RPC_URL, "[RPC]"
            )

        log(
            "FATAL:",
            type(exc).__name__,
            message,
        )

        raise
