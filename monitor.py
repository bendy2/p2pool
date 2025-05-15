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
from api_server import load_config

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


class TariBlockChecker(threading.Thread):
    def __init__(self, db_config):
        super().__init__()
        self.daemon = True
        self.running = True
        self.db_config = db_config
        self.api_url = "https://explore.tari.com/blocks/{height}?json"
        self.check_interval = 60  # 检查间隔（秒）

    def buffer_to_hex(self, buffer_data):
        """将 Buffer 数据转换为十六进制字符串"""
        if not isinstance(buffer_data, dict) or 'data' not in buffer_data:
            return ''
        return ''.join([f'{x:02x}' for x in buffer_data['data']])
    def get_block_data(block_height: int) -> Dict[str, Any]:
        """获取指定高度的区块数据"""
        url = f'https://textexplore.tari.com/blocks/{block_height}?json'
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"获取区块数据失败: {e}")
            return None
    def get_block_from_api(self, height):
        """从 API 获取区块数据"""
        try:
            url = f'https://textexplore.tari.com/blocks/{height}?json'
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # 检查响应内容类型
            content_type = response.headers.get('content-type', '')
            if 'application/json' not in content_type:
                logger.warning(f"API 响应不是 JSON 格式: {content_type}")
                return None
                
            # 尝试解析 JSON
            try:
                data = response.json()
                if not data:
                    logger.warning(f"API 返回空数据: {response.text[:100]}")
                    return None
                return data
            except json.JSONDecodeError as e:
                logger.warning(f"JSON 解析错误: {e}, 响应内容: {response.text[:100]}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"获取区块 {height} 数据失败: {e}")
            return None
        except Exception as e:
            logger.error(f"处理 API 响应时发生未知错误: {e}")
            return None


    def update_block_status(self, block_id, is_valid, remote_hash=None):
        """更新区块状态"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            if is_valid:
                cur.execute("""
                    UPDATE blocks 
                    SET check_status = true, 
                        is_valid = true
                    WHERE id = %s
                """, (block_id,))
            else:
                cur.execute("""
                    UPDATE blocks 
                    SET check_status = true, 
                        is_valid = false
                    WHERE id = %s
                """, (block_id,))
            
            conn.commit()
            logger.info(f"区块 {block_id} 状态已更新: is_valid={is_valid} remote_hash={remote_hash}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"更新区块状态失败: {e}")
        finally:
            cur.close()
            conn.close()

    def check_block(self):
        """检查一个区块"""
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, block_height, block_id 
            FROM blocks 
            WHERE check_status = false 
            AND type = 'tari'
            ORDER BY block_height ASC 
            LIMIT 1
        """)
        block = cur.fetchone()
        if not block:
            logger.info("没有需要检查的区块")
            return
        block_hash = block[2]

        logger.info(f"开始检查区块 {block[1]}")  # block[1] 是 block_height
        api_data = self.get_block_from_api(block[1])
        
        if not api_data:
            logger.info(f"远程未找到区块 {block[1]}，跳过")
            return

        try:
            header = api_data.get('header', {})
            remote_hash = self.buffer_to_hex(header.get('hash', {}))
            
            if not remote_hash or remote_hash != block_hash:
                logger.warning(f"区块 {block[1]} 远程哈希无效")
                self.handle_invalid_block(block[0], block[1])
                return

            # 更新区块状态
            self.update_block_status(block[0], True, remote_hash)
            logger.info(f"区块 {block[1]} 验证成功")

        except Exception as e:
            logger.error(f"检查区块 {block[1]} 时发生错误: {e}")
            # 如果发生错误，将区块标记为无效
            self.handle_invalid_block(block[0], block[1])

    def handle_invalid_block(self, block_id, block_height):
        """处理无效区块"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            # 1. 更新区块状态为无效
            cur.execute("""
                UPDATE blocks 
                SET check_status = true, 
                    is_valid = false
                WHERE id = %s
            """, (block_id,))
            
            # 2. 获取该区块的所有奖励记录
            cur.execute("""
                SELECT username, reward, type
                FROM rewards 
                WHERE block_height = %s
            """, (block_height,))
            rewards = cur.fetchall()
            
            # 3. 回滚用户余额
            for reward in rewards:
                username, amount, reward_type = reward
                if reward_type == 'tari':
                    cur.execute("""
                        UPDATE account 
                        SET tari_balance = tari_balance - %s
                        WHERE username = %s
                    """, (amount, username))
                elif reward_type == 'xmr':
                    cur.execute("""
                        UPDATE account 
                        SET xmr_balance = xmr_balance - %s
                        WHERE username = %s
                    """, (amount, username))
            
            # 4. 删除奖励记录
            cur.execute("""
                UPDATE rewards 
                SET reward = 0
                WHERE block_height = %s
            """, (block_height,))
            
            
            conn.commit()
            logger.info(f"区块 {block_height} 已标记为无效并清理相关数据")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"处理无效区块 {block_height} 时发生错误: {e}")
        finally:
            cur.close()
            conn.close()

    def run(self):
        """运行检查器"""
        while self.running:
            try:
                self.check_block()
            except Exception as e:
                logger.error(f"检查器运行错误: {e}")
            time.sleep(self.check_interval)

    def stop(self):
        """停止检查器"""
        self.running = False




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
                handle_xmr_block({'height': height, 'reward': reward})
                return
                
            # 检查 TARI 爆块信息
            tari_match = self.tari_block_pattern.search(line)
            if tari_match:
                height = int(tari_match.group(2))
                block_id = tari_match.group(1)
                logger.info(f"检测到 TARI 爆块 - 高度: {height}, 区块ID: {block_id}")
                # 直接调用处理函数，让处理函数进行数据库检查
                handle_tari_block({'height': height, 'block_id': block_id})
                
        except Exception as e:
            logger.error(f"处理日志行时出错: {str(e)}")
            
    def stop(self):
        self.running = False



def process_block(block_data):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 设置check_status
        check_status = block_data['type'].lower() == 'xmr'
        
        cur.execute("""
            INSERT INTO blocks (block_id, height, timestamp, type, reward, is_valid, check_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            block_data['block_id'],
            block_data['height'],
            block_data['timestamp'],
            block_data['type'],
            block_data['reward'],
            True,
            check_status
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return True
    except Exception as e:
        logger.error(f"处理区块时发生错误: {str(e)}")
        return False
    

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
            await asyncio.sleep(3)
            
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