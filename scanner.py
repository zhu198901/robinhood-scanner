
import os
import sys
import json
import time
from pathlib import Path

import requests
from web3 import Web3

# Robinhood Chain Scanner - Read Only
RPC_URL = os.getenv("ALCHEMY_RPC_URL", "").strip()
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "").strip()

CHAIN_ID = 4663
STATE_FILE = Path("state.json")

FACTORIES = [
    "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA",
    "0x221A6239E40709792b0d4bdc140fA36158CD41C7",
]

TOPIC = "0x" + Web3.keccak(
    text="PoolCreated(address,address,uint24,int24,address)"
).hex().removeprefix("0x")

CHUNK_SIZE = 10
MAX_BLOCKS_PER_RUN = 300
MAX_LAG_BLOCKS = 200000
MAX_PUSHES = 8
MIN_LIQUIDITY = 300000
MIN_VOLUME = 100000
MIN_TXNS = 20
MAX_AGE_HOURS = 72
TIMEOUT = 20

session = requests.Session()


def log(message):
    print(
        time.strftime("[%Y-%m-%d %H:%M:%S] ") + str(message),
        flush=True
    )


def safe_error(message):
    message = str(message)
    for secret in (SERVER_CHAN_KEY, RPC_URL):
        if secret:
            message = message.replace(secret, "[REDACTED]")
    return message


def rpc(method, params=None):
    if not RPC_URL:
        raise RuntimeError("Missing ALCHEMY_RPC_URL")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or [],
    }

    for attempt in range(5):
        try:
            response = session.post(
                RPC_URL, json=payload, timeout=TIMEOUT
            )
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                raise RuntimeError(str(data["error"]))

            if "result" not in data:
                raise RuntimeError("Missing RPC result")

            return data["result"]

        except Exception as exc:
            log("RPC error: " + safe_error(exc))
            if attempt == 4:
                raise
            time.sleep(min(2 ** attempt, 16))

    raise RuntimeError("RPC retries exhausted")


def send_wechat(title, body):
    if not SERVER_CHAN_KEY:
        log("Missing SERVER_CHAN_KEY")
        return False

    url = (
        "https://sctapi.ftqq.com/"
        + SERVER_CHAN_KEY
        + ".send"
    )

    for attempt in range(3):
        try:
            response = session.post(
                url,
                data={
                    "title": title,
                    "desp": body,
                },
                timeout=TIMEOUT,
            )

            if response.status_code != 200:
                log(
                    "ServerChan HTTP "
                    + str(response.status_code)
                    + ": "
                    + safe_error(response.text[:500])
                )

                if response.status_code in (400, 401, 403):
                    return False

                time.sleep(2 ** attempt)
                continue

            try:
                result = response.json()
            except ValueError:
                log("ServerChan returned invalid JSON")
                return False

            if result.get("code") == 0:
                log("WeChat push success")
                return True

            log(
                "ServerChan API response: "
                + safe_error(result)
            )

            if result.get("code") not in (429, 500, 503):
                return False

        except requests.RequestException as exc:
            log("WeChat request error: " + safe_error(exc))

        time.sleep(2 ** attempt)

    return False


def load_state():
    default = {
        "last_block": None,
        "seen": [],
        "pending": [],
        "skipped_ranges": [],
    }

    if not STATE_FILE.exists():
        return default

    try:
        data = json.loads(
            STATE_FILE.read_text(encoding="utf-8")
        )

        if not isinstance(data, dict):
            raise ValueError("Invalid state")

        for key, value in default.items():
            data.setdefault(key, value)

        for key in ("seen", "pending", "skipped_ranges"):
            if not isinstance(data[key], list):
                data[key] = []

        return data

    except Exception as exc:
        raise RuntimeError(
            "Cannot load state: " + safe_error(exc)
        )


def save_state(state):
    state["seen"] = list(
        dict.fromkeys(state["seen"])
    )[-10000:]

    state["pending"] = state["pending"][-3000:]
    state["skipped_ranges"] = state["skipped_ranges"][-100:]

    temp = Path("state.tmp")
    temp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    temp.replace(STATE_FILE)


def decode_address(value):
    try:
        return Web3.to_checksum_address(
            "0x" + value[-40:]
        )
    except Exception:
        return None


def scan_events(start, end):
    result = rpc(
        "eth_getLogs",
        [{
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "address": FACTORIES,
            "topics": [TOPIC],
        }]
    )

    events = []

    for item in result:
        try:
            topics = item["topics"]
            if len(topics) < 3:
                continue

            token0 = decode_address(topics[1])
            token1 = decode_address(topics[2])

            raw = item["data"].removeprefix("0x")
            if len(raw) < 128:
                continue

            pool = decode_address(raw[64:128])
            tx = item["transactionHash"]
            index = int(item["logIndex"], 16)
            block = int(item["blockNumber"], 16)

            if not all((token0, token1, pool)):
                continue

            events.append({
                "id": tx.lower() + ":" + str(index),
                "block": block,
                "tx": tx,
                "pool": pool,
                "token0": token0,
                "token1": token1,
                "first_seen": int(time.time()),
            })

        except (KeyError, ValueError, TypeError) as exc:
            log("Invalid pool event: " + safe_error(exc))

    return events


def fetch_pairs(tokens):
    tokens = list(dict.fromkeys(
        token.lower() for token in tokens if token
    ))

    pairs = []
    success = True

    for token in tokens:
        url = (
            "https://api.dexscreener.com/"
            "latest/dex/tokens/" + token
        )

        try:
            response = session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict):
                success = False
                continue

            result = data.get("pairs") or []
            if not isinstance(result, list):
                success = False
                continue

            for pair in result:
                if (
                    isinstance(pair, dict)
                    and str(pair.get("chainId", "")).lower()
                    == "robinhood"
                ):
                    pairs.append(pair)

        except Exception as exc:
            success = False
            log("Market API error: " + safe_error(exc))

        time.sleep(0.25)

    unique = {}

    for pair in pairs:
        address = str(pair.get("pairAddress", "")).lower()
        if address:
            unique[address] = pair

    return list(unique.values()), success


def evaluate_pair(pair, event):
    try:
        pair_address = str(
            pair.get("pairAddress", "")
        ).lower()

        if pair_address != event["pool"].lower():
            return None

        liquidity = float(
            (pair.get("liquidity") or {}).get("usd") or 0
        )
        volume = float(
            (pair.get("volume") or {}).get("h24") or 0
        )

        h24 = (pair.get("txns") or {}).get("h24") or {}
        buys = int(h24.get("buys") or 0)
        sells = int(h24.get("sells") or 0)

        created = int(pair.get("pairCreatedAt") or 0)

        if not created:
            return None

        age = max(
            0,
            (time.time() * 1000 - created) / 3600000
        )

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
        if volume / max(liquidity, 1) > 5:
            return None
        if age > MAX_AGE_HOURS:
            return None

        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}

        token = base

        if (
            str(base.get("address", "")).lower()
            not in (
                event["token0"].lower(),
                event["token1"].lower(),
            )
        ):
            token = quote

        score = min(
            100,
            int(
                min(30, liquidity / 300000 * 15)
                + min(25, volume / 100000 * 10)
                + min(20, (buys + sells) / 20 * 5)
                + (15 if buys >= sells else 5)
                + (10 if age <= 24 else 5)
            )
        )

        return {
            "pair": pair,
            "token": token,
            "liquidity": liquidity,
            "volume": volume,
            "buys": buys,
            "sells": sells,
            "age": age,
            "score": score,
        }

    except (TypeError, ValueError, ZeroDivisionError):
        return None


def format_candidate(event, candidate):
    token = candidate["token"]
    pair = candidate["pair"]

    name = token.get("name") or "Unknown"
    symbol = token.get("symbol") or "?"
    address = token.get("address") or ""

    title = (
        f"Robinhood候选币：{symbol} "
        f"评分{candidate['score']}"
    )

    body = "\n".join([
        f"代币：{name} ({symbol})",
        f"合约：{address}",
        f"交易池：{event['pool']}",
        f"DEX：{pair.get('dexId', 'Unknown')}",
        f"流动性：${candidate['liquidity']:,.0f}",
        f"24小时成交量：${candidate['volume']:,.0f}",
        f"买入笔数：{candidate['buys']}",
        f"卖出笔数：{candidate['sells']}",
        f"池龄：{candidate['age']:.1f}小时",
        f"评分：{candidate['score']}/100",
        f"区块：{event['block']}",
        f"行情：{pair.get('url', '')}",
        "",
        "仅供候选观察，不代表安全或买入建议。",
        "请核查合约权限、持币集中度及LP风险。",
    ])

    return title, body


def main():
    log("Starting Robinhood Chain Scanner")

    chain = int(rpc("eth_chainId"), 16)

    if chain != CHAIN_ID:
        raise RuntimeError(
            f"Wrong chain: expected {CHAIN_ID}, got {chain}"
        )

    latest = int(rpc("eth_blockNumber"), 16)
    log(f"Connected. Latest block: {latest}")

    state = load_state()
    last = state["last_block"]

    if last is None:
        start = max(0, latest - MAX_BLOCKS_PER_RUN + 1)
    else:
        last = int(last)

        if last > latest:
            last = max(0, latest - MAX_BLOCKS_PER_RUN)

        if latest - last > MAX_LAG_BLOCKS:
            new_start = max(
                0, latest - MAX_BLOCKS_PER_RUN + 1
            )
            state["skipped_ranges"].append({
                "from": last + 1,
                "to": new_start - 1,
                "recorded_at": int(time.time()),
                "reason": "backlog safety limit",
            })
            last = new_start - 1
            log("Historical backlog skipped")

        start = last + 1

    end = min(
        latest,
        start + MAX_BLOCKS_PER_RUN - 1
    )

    seen = set(state["seen"])
    pending_ids = {
        e.get("id")
        for e in state["pending"]
        if isinstance(e, dict)
    }

    for block in range(start, end + 1, CHUNK_SIZE):
        chunk_end = min(
            block + CHUNK_SIZE - 1, end
        )

        events = scan_events(block, chunk_end)

        for event in events:
            if (
                event["id"] not in seen
                and event["id"] not in pending_ids
            ):
                state["pending"].append(event)
                pending_ids.add(event["id"])

        state["last_block"] = chunk_end
        save_state(state)

        log(
            f"Scanned {block}-{chunk_end}, "
            f"events={len(events)}"
        )
        time.sleep(0.8)

    now = int(time.time())
    active = []

    for event in state["pending"]:
        first = int(event.get("first_seen") or now)

        if now - first > 86400:
            state["seen"].append(event["id"])
        else:
            active.append(event)

    state["pending"] = active

    if not active:
        save_state(state)
        log("Scan complete: no pending pools")
        return

    tokens = []

    for event in active:
        tokens.extend([
            event["token0"],
            event["token1"],
        ])

    pairs, api_ok = fetch_pairs(tokens)

    if not api_ok:
        save_state(state)
        log("Market API incomplete; retry next run")
        return

    pair_map = {
        str(p.get("pairAddress", "")).lower(): p
        for p in pairs
    }

    remaining = []
    pushed = 0

    for event in active:
        if pushed >= MAX_PUSHES:
            remaining.append(event)
            continue

        pair = pair_map.get(event["pool"].lower())

        if pair is None:
            remaining.append(event)
            continue

        candidate = evaluate_pair(pair, event)

        if candidate:
            title, body = format_candidate(
                event, candidate
            )

            if send_wechat(title, body):
                pushed += 1
                state["seen"].append(event["id"])
            else:
                remaining.append(event)
        else:
            state["seen"].append(event["id"])

    state["pending"] = remaining
    save_state(state)

    log(
        f"Run complete | pushed={pushed} | "
        f"pending={len(remaining)} | "
        f"last_block={state['last_block']}"
    )


if __name__ == "__main__":
    try:
        if os.getenv("TEST_PUSH") == "1":
            log("Starting WeChat push test")

            ok = send_wechat(
                "Robinhood 扫链机器人测试",
                "微信推送测试消息。\n"
                "本消息不是买币信号。\n"
                "机器人没有执行任何交易。"
            )

            if ok:
                log("TEST PUSH SUCCESS")
                sys.exit(0)
            else:
                log("TEST PUSH FAILED")
                sys.exit(1)

        main()

    except Exception as exc:
        log("FATAL: " + safe_error(exc))
        sys.exit(1)
