import asyncio
import json
import os
import requests
from datetime import datetime, timezone
from web3 import AsyncWeb3, AsyncHTTPProvider

STATE_FILE = "state.json"
DATA_FILE = "scanned_data.json"
POLL_INTERVAL = 5

# 只保留 Robinhood 链配置
CHAINS = {
    "Robinhood": {
        "rpc": "https://rpc.robinhood.com",  # 确保是正确的Robinhood RPC地址
        "target_keywords": [] # 可以根据需要添加过滤关键词
    }
}

def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default

def save_json(path, data):
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(path + ".tmp", path)

def send_wechat_notification(title, desp):
    server_key = os.environ.get("SERVER_CHAN_KEY")
    if not server_key:
        return
    url = f"https://sctapi.ftqq.com/{server_key}.send"
    data = {"title": title, "desp": desp}
    try:
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print(f"微信通知发送失败: {e}")

async def scan_chain(name, config, state, lock):
    while True:
        try:
            w3 = AsyncWeb3(AsyncHTTPProvider(config["rpc"], request_kwargs={"timeout": 20}))
            if not await w3.is_connected():
                raise ConnectionError("RPC 无法连接")

            async with lock:
                last_block = state.get(name)
                if last_block is None:
                    last_block = await w3.eth.block_number
                    state[name] = last_block
                    save_json(STATE_FILE, state)

            while True:
                latest = await w3.eth.block_number
                if latest > last_block:
                    block_number = last_block + 1
                    block = await w3.eth.get_block(block_number, full_transactions=True)
                    print(f"[{name}] 区块-{block_number} | 交易数={len(block['transactions'])}")
                    
                    # 示例：如果发现特定交易或满足条件，可直接调用推送
                    # send_wechat_notification(f"发现新交易", f"链: {name}, 区块: {block_number}")

                    record = {
                        "chain": name,
                        "block_number": block_number,
                        "timestamp": datetime.fromtimestamp(block["timestamp"], tz=timezone.utc).isoformat()
                    }
                    async with lock:
                        records = load_json(DATA_FILE, [])
                        records.append(record)
                        save_json(DATA_FILE, records)
                        state[name] = block_number
                        save_json(STATE_FILE, state)
                        last_block = block_number
                else:
                    await asyncio.sleep(POLL_INTERVAL)
        except Exception as exc:
            print(f"[{name}] 扫描异常: {exc}; 5秒后重连")
            await asyncio.sleep(5)

async def main():
    state = load_json(STATE_FILE, {})
    lock = asyncio.Lock()
    tasks = [asyncio.create_task(scan_chain(n, c, state, lock)) for n, c in CHAINS.items()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
