import json
import os
import requests
from datetime import datetime, timezone
from web3 import Web3

RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
CHAIN_ID = 4663
FACTORY = Web3.to_checksum_address(
    "0x1E3aC31cF96b20619c913384C9bf6010A824fB95"
)
STATE_FILE = "state.json"
DATA_FILE = "scanned_data.json"
BATCH_SIZE = 300
FIRST_LOOKBACK = 1000

PAIR_TOPIC = Web3.keccak(
    text="PairCreated(address,address,address,uint256)"
).hex()


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return default


def save_json(path, data):
    temp = path + ".tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(temp, path)


def hex_text(value):
    text = value.hex() if hasattr(value, "hex") else str(value)
    return text.removeprefix("0x")


def address_from_topic(topic):
    return Web3.to_checksum_address(
        "0x" + hex_text(topic)[-40:]
    )


def address_from_data(data):
    value = hex_text(data)
    if len(value) < 64:
        raise ValueError("PairCreated data长度不足")
    return Web3.to_checksum_address("0x" + value[24:64])


def send_notification(title, message):
    key = os.environ.get("SERVER_CHAN_KEY", "").strip()
    if not key:
        print("未配置 SERVER_CHAN_KEY")
        return False

    try:
        response = requests.post(
            f"https://sctapi.ftqq.com/{key}.send",
            data={"title": title[:32], "desp": message},
            timeout=15,
        )
        print("推送接口返回：", response.text[:300])
        response.raise_for_status()
        return response.json().get("code") == 0
    except Exception as exc:
        print("微信推送失败：", exc)
        return False
    def main():
    w3 = Web3(Web3.HTTPProvider(
        RPC_URL, request_kwargs={"timeout": 25}
    ))

    if not w3.is_connected():
        raise RuntimeError("Robinhood RPC连接失败")

    chain_id = w3.eth.chain_id
    print("RPC连接成功，Chain ID =", chain_id)
    if chain_id != CHAIN_ID:
        raise RuntimeError(f"链ID不匹配：{chain_id}")

    latest = w3.eth.block_number
    state = load_json(STATE_FILE, {})
    last_block = state.get("Robinhood")

    if last_block is None:
        last_block = max(0, latest - FIRST_LOOKBACK)
        print("首次扫描，回看最近1000个区块")
    else:
        last_block = int(last_block)

    if latest <= last_block:
        print("没有新区块，当前区块：", latest)
        return

    records = load_json(DATA_FILE, [])
    if not isinstance(records, list):
        records = []

    found = []
    start = last_block + 1
    batch_size = BATCH_SIZE

    while start <= latest:
        end = min(start + batch_size - 1, latest)
        print(f"扫描区块 {start} 到 {end}")

        try:
            logs = w3.eth.get_logs({
                "fromBlock": start,
                "toBlock": end,
                "address": FACTORY,
                "topics": [PAIR_TOPIC],
            })
        except Exception as exc:
            if end > start:
                batch_size = max(1, (end - start + 1) // 2)
                print("查询失败，缩小范围重试：", exc)
                continue
            raise

        for log in logs:
            try:
                block_no = int(log["blockNumber"])
                block = w3.eth.get_block(block_no)

                item = {
                    "chain": "Robinhood",
                    "dex": "Topaz V2",
                    "block_number": block_no,
                    "time_utc": datetime.fromtimestamp(
                        block["timestamp"], tz=timezone.utc
                    ).isoformat(),
                    "token0": address_from_topic(log["topics"][1]),
                    "token1": address_from_topic(log["topics"][2]),
                    "pair": address_from_data(log["data"]),
                    "tx_hash": hex_text(log["transactionHash"]),
                }

                found.append(item)
                records.append(item)
                print("发现新池：", json.dumps(item))

            except Exception as exc:
                print("解析日志失败：", exc)

        start = end + 1

    save_json(DATA_FILE, records[-5000:])
    save_json(STATE_FILE, {"Robinhood": latest})

    print(f"扫描完成，最新区块={latest}，新池数量={len(found)}")

    if found:
        lines = [
            "Robinhood Chain",
            "DEX：Topaz V2",
            f"发现新池：{len(found)}个",
            "",
        ]

        for item in found[:10]:
            lines.extend([
                f"区块：{item['block_number']}",
                f"Token0：{item['token0']}",
                f"Token1：{item['token1']}",
                f"池子：{item['pair']}",
                f"交易：{item['tx_hash']}",
                "",
            ])

        sent = send_notification(
            "Robinhood发现新交易池",
            "\n".join(lines),
        )
        print("微信推送结果：", "成功" if sent else "失败")
    else:
        print("本次没有发现Topaz V2新池。")
        print("这不代表整条Robinhood Chain没有新币。")


if __name__ == "__main__":
    main()
