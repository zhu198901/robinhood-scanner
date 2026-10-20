import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider
from web3.providers.persistent import WebSocketProvider

CHAINS = {
    "Robinhood": {"rpc": "https://rpc.mainnet.chain.robinhood.com", "chain_id": 4663},
    "BSC": {"rpc": "https://1rpc.io/bnb", "chain_id": 56},
}

POLL_INTERVAL = 5
DATA_FILE = "discovered_pools.json"
STATE_FILE = "scanner_state.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scanner")

def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default

def save_json(path, data):
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(path + ".tmp", path)

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
                async with lock:
                    state[name] = last_block
                    save_json(STATE_FILE, state)

            while True:
                latest = await w3.eth.block_number
                if latest > last_block:
                    block_number = last_block + 1
                    block = await w3.eth.get_block(block_number, full_transactions=True)
                    log.info("%s 区块=%s | 交易数=%s", name, block_number, len(block["transactions"]))
                    
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
            log.error("%s 扫描异常：%s；5秒后重连", name, exc)
            await asyncio.sleep(5)

async def main():
    state = load_json(STATE_FILE, {})
    lock = asyncio.Lock()
    tasks = [asyncio.create_task(scan_chain(n, c, state, lock)) for n, c in CHAINS.items()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
