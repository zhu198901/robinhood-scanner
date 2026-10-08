
import asyncio
import logging
import os
import sys
from datetime import datetime
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider, WebSocketProvider

# 配置日志打印格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("RobinhoodScanner")

# 获取环境变量中的 ALCHEMY_RPC_URL (支持 https:// 或 wss://)
RPC_URL = os.getenv("ALCHEMY_RPC_URL")

# ERC-20 Transfer 事件签名哈希: Transfer(address,address,uint256)
TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# 零地址，常用于判断代币 Initial Mint (创建/铸造)
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


async def handle_new_block(w3: AsyncWeb3, block_number: int):
    """处理新区块，扫描该区块内的日志与新币痕迹"""
    try:
        block = await w3.eth.get_block(block_number, full_transactions=True)
        block_time = datetime.fromtimestamp(block["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")

        # 获取该区块内从 0x0 (Mint) 发出的 ERC-20 Transfer 日志
        logs = await w3.eth.get_logs(
            {
                "fromBlock": block_number,
                "toBlock": block_number,
                "topics": [
                    TRANSFER_EVENT_SIGNATURE,
                    "0x0000000000000000000000000000000000000000000000000000000000000000",  # from = 0x0
                ],
            }
        )

        for log in logs:
            token_address = log["address"]
            tx_hash = log["transactionHash"].hex()
            logger.info("=" * 60)
            logger.info("🔥 检测到疑似新 Token 部署/初始 Mint！")
            logger.info(f"   ► 代币合约 (Token): {token_address}")
            logger.info(f"   ► 区块高度 (Block): {block_number}")
            logger.info(f"   ► 发现时间 (Time):  {block_time}")
            logger.info(f"   ► 交易 Hash (Tx):   {tx_hash}")
            logger.info("=" * 60)

    except Exception as e:
        logger.error(f"处理区块 #{block_number} 时出错: {e}")


async def main():
    if not RPC_URL:
        logger.error("❌ 错误: 未设置环境变量 ALCHEMY_RPC_URL")
        sys.exit(1)

    logger.info("正在尝试连接 Robinhood Chain 节点...")

    # 根据 URL 前缀选择适配器 (WSS 或 HTTP)
    if RPC_URL.startswith("wss://") or RPC_URL.startswith("ws://"):
        async_provider = WebSocketProvider(RPC_URL)
    else:
        async_provider = AsyncHTTPProvider(RPC_URL)

    async with AsyncWeb3(async_provider) as w3:
        is_connected = await w3.is_connected()
        if not is_connected:
            logger.error("❌ 无法连接到 Alchemy 节点，请检查 ALCHEMY_RPC_URL 设置。")
            return

        chain_id = await w3.eth.chain_id
        latest_block = await w3.eth.block_number
        logger.info(f"✅ 成功连接至节点 | Chain ID: {chain_id} | 当前最新高度: {latest_block}")
        logger.info("🚀 扫描器已启动，正在实时监听新区块...")

        last_processed_block = latest_block

        # 持续监听轮询循环
        while True:
            try:
                current_block = await w3.eth.block_number
                if current_block > last_processed_block:
                    for b in range(last_processed_block + 1, current_block + 1):
                        await handle_new_block(w3, b)
                    last_processed_block = current_block

                await asyncio.sleep(1)  # 轮询间隔 1 秒
            except Exception as e:
                logger.error(f"监听主循环发生异常: {e}")
                await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("手动终止扫描器进程。")
