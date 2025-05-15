import asyncio
import logging
import re
import threading
import time
from queue import Queue
import aiohttp
import asyncpg
from typing import Dict, Any

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'p2pool'
}

# 全局数据库连接池
db_pool = None

class LogMonitorThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.running = True
        self.log_queue = Queue()
        self.log_file = './p2pool.log'
        
        # 编译正则表达式模式
        self.xmr_block_pattern = re.compile(r'got a payout of ([\d.]+) XMR in block (\d+)')
        self.tari_block_pattern = re.compile(r'Mined Tari block ([a-f0-9]+) at height (\d+)')
        
    def run(self):
        try:
            # 打开日志文件
            with open(self.log_file, 'r') as f:
                # 移动到文件末尾
                f.seek(0, 2)
                
                while self.running:
                    line = f.readline()
                    if not line:
                        # 如果没有新内容，等待一小段时间
                        time.sleep(0.1)
                        continue
                        
                    # 将日志行放入队列
                    self.log_queue.put(line)
                    
                    # 处理日志行
                    self.process_log_line(line)
                    
        except Exception as e:
            logger.error(f"日志监控线程错误: {str(e)}")
            
    def process_log_line(self, line):
        try:
            # 检查 XMR 爆块信息
            xmr_match = self.xmr_block_pattern.search(line)
            if xmr_match:
                reward = float(xmr_match.group(1))
                height = int(xmr_match.group(2))
                logger.info(f"检测到 XMR 爆块 - 高度: {height}, 奖励: {reward}")
                # 直接调用处理函数，让处理函数进行数据库检查
                asyncio.create_task(handle_xmr_block({'height': height, 'reward': reward}))
                return
                
            # 检查 TARI 爆块信息
            tari_match = self.tari_block_pattern.search(line)
            if tari_match:
                height = int(tari_match.group(2))
                block_id = tari_match.group(1)
                logger.info(f"检测到 TARI 爆块 - 高度: {height}, 区块ID: {block_id}")
                # 直接调用处理函数，让处理函数进行数据库检查
                asyncio.create_task(handle_tari_block({'height': height, 'block_id': block_id}))
                
        except Exception as e:
            logger.error(f"处理日志行时出错: {str(e)}")
            
    def stop(self):
        self.running = False

class TariBlockChecker(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.running = True
        self.check_interval = 60  # 检查间隔（秒）

    def buffer_to_hex(self, buffer_data):
        """将 Buffer 数据转换为十六进制字符串"""
        if not isinstance(buffer_data, dict) or 'data' not in buffer_data:
            return ''
        return ''.join([f'{x:02x}' for x in buffer_data['data']])

    async def get_block_from_api(self, height):
        """从 API 获取区块数据"""
        try:
            url = f'https://textexplore.tari.com/blocks/{height}?json'
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    response.raise_for_status()
                    
                    # 检查响应内容类型
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' not in content_type:
                        logger.warning(f"API 响应不是 JSON 格式: {content_type}")
                        return None
                        
                    # 尝试解析 JSON
                    try:
                        data = await response.json()
                        if not data:
                            logger.warning(f"API 返回空数据: {await response.text()[:100]}")
                            return None
                        return data
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON 解析错误: {e}, 响应内容: {await response.text()[:100]}")
                        return None
                    
        except Exception as e:
            logger.error(f"处理 API 响应时发生未知错误: {e}")
            return None

    async def update_block_status(self, block_id, is_valid, remote_hash=None):
        """更新区块状态"""
        try:
            async with db_pool.acquire() as conn:
                if is_valid:
                    await conn.execute("""
                        UPDATE blocks 
                        SET check_status = true, 
                            is_valid = true
                        WHERE id = $1
                    """, block_id)
                else:
                    await conn.execute("""
                        UPDATE blocks 
                        SET check_status = true, 
                            is_valid = false
                        WHERE id = $1
                    """, block_id)
                
                logger.info(f"区块 {block_id} 状态已更新: is_valid={is_valid} remote_hash={remote_hash}")
                
        except Exception as e:
            logger.error(f"更新区块状态失败: {e}")

    async def check_block(self):
        """检查一个区块"""
        async with db_pool.acquire() as conn:
            block = await conn.fetchrow("""
                SELECT id, block_height, block_id 
                FROM blocks 
                WHERE check_status = false 
                AND type = 'tari'
                ORDER BY block_height ASC 
                LIMIT 1
            """)
            
            if not block:
                logger.info("没有需要检查的区块")
                return
                
            block_hash = block['block_id']
            logger.info(f"开始检查区块 {block['block_height']}")
            
            api_data = await self.get_block_from_api(block['block_height'])
            
            if not api_data:
                logger.info(f"远程未找到区块 {block['block_height']}，跳过")
                return

            try:
                header = api_data.get('header', {})
                remote_hash = self.buffer_to_hex(header.get('hash', {}))
                
                if not remote_hash or remote_hash != block_hash:
                    logger.warning(f"区块 {block['block_height']} 远程哈希无效")
                    await self.handle_invalid_block(block['id'], block['block_height'])
                    return

                # 更新区块状态
                await self.update_block_status(block['id'], True, remote_hash)
                logger.info(f"区块 {block['block_height']} 验证成功")

            except Exception as e:
                logger.error(f"检查区块 {block['block_height']} 时发生错误: {e}")
                # 如果发生错误，将区块标记为无效
                await self.handle_invalid_block(block['id'], block['block_height'])

    async def handle_invalid_block(self, block_id, block_height):
        """处理无效区块"""
        try:
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    # 1. 更新区块状态为无效
                    await conn.execute("""
                        UPDATE blocks 
                        SET check_status = true, 
                            is_valid = false
                        WHERE id = $1
                    """, block_id)
                    
                    # 2. 获取该区块的所有奖励记录
                    rewards = await conn.fetch("""
                        SELECT username, reward, type
                        FROM rewards 
                        WHERE block_height = $1
                    """, block_height)
                    
                    # 3. 回滚用户余额
                    for reward in rewards:
                        if reward['type'] == 'tari':
                            await conn.execute("""
                                UPDATE account 
                                SET tari_balance = tari_balance - $1
                                WHERE username = $2
                            """, reward['reward'], reward['username'])
                        elif reward['type'] == 'xmr':
                            await conn.execute("""
                                UPDATE account 
                                SET xmr_balance = xmr_balance - $1
                                WHERE username = $2
                            """, reward['reward'], reward['username'])
                    
                    # 4. 删除奖励记录
                    await conn.execute("""
                        UPDATE rewards 
                        SET reward = 0
                        WHERE block_height = $1
                    """, block_height)
                    
                    logger.info(f"区块 {block_height} 已标记为无效并清理相关数据")
                    
        except Exception as e:
            logger.error(f"处理无效区块 {block_height} 时发生错误: {e}")

    def run(self):
        """运行检查器"""
        while self.running:
            try:
                asyncio.run(self.check_block())
            except Exception as e:
                logger.error(f"检查器运行错误: {e}")
            time.sleep(self.check_interval)

    def stop(self):
        """停止检查器"""
        self.running = False

async def init_db():
    """初始化数据库连接池"""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(**DB_CONFIG)
        logger.info("Successfully connected to database")
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

async def handle_xmr_block(block_data: Dict[str, Any]):
    """处理 XMR 区块数据"""
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            # 检查区块是否已存在
            cur.execute("""
                SELECT id FROM blocks 
                WHERE block_height = %s AND type = 'xmr'
            """, (block_data['height'],))
            
            if cur.fetchone():
                logger.info(f"XMR 区块 {block_data['height']} 已存在")
                return
                
            # 插入新区块
            cur.execute("""
                INSERT INTO blocks (block_height, block_id, type, check_status, is_valid)
                VALUES (%s, %s, 'xmr', true, true)
                RETURNING id
            """, (block_data['height'], str(block_data['height'])))
            
            # 获取插入的区块ID
            block_id = cur.fetchone()
            if not block_id:
                logger.error(f"插入 XMR 区块 {block_data['height']} 失败，未返回区块ID")
                conn.rollback()
                return
                
            block_id = block_id[0]  # 获取返回的ID值
            
            # 获取该区块的奖励信息
            cur.execute("""
                SELECT username, reward, type
                FROM rewards 
                WHERE block_height = %s
            """, (block_data['height'],))
            
            rewards = cur.fetchall()
            if not rewards:
                logger.warning(f"XMR 区块 {block_data['height']} 没有找到奖励记录")
                conn.commit()
                return
                
            # 更新用户余额
            for reward in rewards:
                username, reward_amount, reward_type = reward
                if reward_type == 'xmr':
                    cur.execute("""
                        UPDATE account 
                        SET xmr_balance = xmr_balance + %s
                        WHERE username = %s
                    """, (reward_amount, username))
                elif reward_type == 'tari':
                    cur.execute("""
                        UPDATE account 
                        SET tari_balance = tari_balance + %s
                        WHERE username = %s
                    """, (reward_amount, username))
            
            conn.commit()
            logger.info(f"XMR 区块 {block_data['height']} 处理完成，区块ID: {block_id}")
            
    except Exception as e:
        logger.error(f"处理 XMR 区块时出错: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            db_pool.putconn(conn)

async def handle_tari_block(block_data: Dict[str, Any]):
    """处理 TARI 区块数据"""
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            # 检查区块是否已存在
            cur.execute("""
                SELECT id FROM blocks 
                WHERE block_height = %s AND type = 'tari'
            """, (block_data['height'],))
            
            if cur.fetchone():
                logger.info(f"TARI 区块 {block_data['height']} 已存在")
                return
                
            # 插入新区块
            cur.execute("""
                INSERT INTO blocks (block_height, block_id, type, check_status, is_valid)
                VALUES (%s, %s, 'tari', false, false)
                RETURNING id
            """, (block_data['height'], block_data['block_id']))
            
            # 获取插入的区块ID
            block_id = cur.fetchone()
            if not block_id:
                logger.error(f"插入 TARI 区块 {block_data['height']} 失败，未返回区块ID")
                conn.rollback()
                return
                
            block_id = block_id[0]  # 获取返回的ID值
            
            # 获取该区块的奖励信息
            cur.execute("""
                SELECT username, reward, type
                FROM rewards 
                WHERE block_height = %s
            """, (block_data['height'],))
            
            rewards = cur.fetchall()
            if not rewards:
                logger.warning(f"TARI 区块 {block_data['height']} 没有找到奖励记录")
                conn.commit()
                return
                
            # 更新用户余额
            for reward in rewards:
                username, reward_amount, reward_type = reward
                if reward_type == 'tari':
                    cur.execute("""
                        UPDATE account 
                        SET tari_balance = tari_balance + %s
                        WHERE username = %s
                    """, (reward_amount, username))
                elif reward_type == 'xmr':
                    cur.execute("""
                        UPDATE account 
                        SET xmr_balance = xmr_balance + %s
                        WHERE username = %s
                    """, (reward_amount, username))
            
            conn.commit()
            logger.info(f"TARI 区块 {block_data['height']} 处理完成，区块ID: {block_id}")
            
    except Exception as e:
        logger.error(f"处理 TARI 区块时出错: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            db_pool.putconn(conn)

async def main():
    """主函数"""
    try:
        # 初始化数据库连接
        await init_db()
        
        # 创建并启动日志监控线程
        log_monitor = LogMonitorThread()
        log_monitor.start()
        
        # 创建并启动区块检查线程
        block_checker = TariBlockChecker()
        block_checker.start()
        
        # 保持程序运行
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("正在关闭程序...")
        log_monitor.stop()
        block_checker.stop()
    except Exception as e:
        logger.error(f"程序运行错误: {str(e)}")
    finally:
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
