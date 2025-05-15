import asyncio
import logging
import json
import threading
import time
import re
from queue import Queue
import aiohttp
from datetime import datetime
import asyncpg
import aioredis
from api_server import LogMonitorThread, TariBlockChecker, load_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('monitor.log')
    ]
)

logger = logging.getLogger(__name__)

async def init_redis():
    """初始化Redis连接"""
    try:
        redis_client = await aioredis.from_url(
            'redis://localhost',
            encoding='utf-8',
            max_connections=10
        )
        logger.info("Successfully connected to Redis")
        return redis_client
    except Exception as e:
        logger.error(f"Redis connection failed: {str(e)}")
        raise

async def init_db():
    """初始化数据库连接池"""
    try:
        config = load_config()
        db_pool = await asyncpg.create_pool(
            host=config['database']['host'],
            port=config['database']['port'],
            user=config['database']['user'],
            password=config['database']['password'],
            database=config['database']['database'],
            min_size=5,
            max_size=20
        )
        logger.info("Successfully connected to PostgreSQL")
        return db_pool
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise


    

async def main():
    """主函数"""
    try:
        # 初始化Redis和数据库连接
        redis_client = await init_redis()
        db_pool = await init_db()
        
        # 加载配置
        config = load_config()
        
        # 创建并启动日志监控线程
        log_monitor = LogMonitorThread()
        log_monitor.start()
        
        # 创建并启动Tari区块检查器
        tari_checker = TariBlockChecker(config['database'])
        tari_checker.start()
        
        # 保持程序运行
        while True:
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Monitor service error: {str(e)}")
        raise
    finally:
        # 确保在程序退出时停止所有线程
        if 'log_monitor' in locals():
            log_monitor.stop()
        if 'tari_checker' in locals():
            tari_checker.stop()
        if 'db_pool' in locals():
            await db_pool.close()
        if 'redis_client' in locals():
            await redis_client.close()

if __name__ == '__main__':
    try:
        # 运行主函数
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Monitor service stopped by user")
    except Exception as e:
        logger.error(f"Monitor service failed: {str(e)}") 