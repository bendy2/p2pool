import asyncio
import logging
import re
import threading
import time
from queue import Queue
import aiohttp
import asyncpg
from typing import Dict, Any
import json
from psycopg2 import pool
import requests

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='monitor.log',  # 使用不同的日志文件
    handlers=[
        logging.FileHandler('monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('monitor')  # 使用不同的日志记录器名称

# 从配置文件加载数据库配置
def load_config():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            return config.get('database', {})
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        raise

# 数据库配置
DB_CONFIG = load_config()

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
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 1  # 重试延迟（秒）

    def buffer_to_hex(self, buffer_data):
        """将 Buffer 数据转换为十六进制字符串"""
        if not isinstance(buffer_data, dict) or 'data' not in buffer_data:
            return ''
        return ''.join([f'{x:02x}' for x in buffer_data['data']])

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
            
        except Exception as e:
            logger.error(f"处理 API 响应时发生未知错误: {e}")
            return None

    def update_block_status(self, block_id, is_valid, remote_hash=None):
        """更新区块状态"""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
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
            logger.error(f"更新区块状态失败: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                try:
                    db_pool.putconn(conn)
                except Exception as e:
                    logger.error(f"释放数据库连接时出错: {str(e)}")

    def check_block(self):
        """检查一个区块"""
        conn = None
        for attempt in range(self.max_retries):
            try:
                conn = get_db_connection()
                with conn.cursor() as cur:
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
                    
                block_id, block_height, block_hash = block
                logger.info(f"开始检查区块 {block_height}")
                
                api_data = self.get_block_from_api(block_height)
                
                if not api_data:
                    logger.info(f"远程未找到区块 {block_height}，跳过")
                    return

                try:
                    header = api_data.get('header', {})
                    remote_hash = self.buffer_to_hex(header.get('hash', {}))
                    
                    if not remote_hash or remote_hash != block_hash:
                        logger.warning(f"区块 {block_height} 远程哈希无效")
                        self.handle_invalid_block(block_id, block_height)
                        return

                    # 更新区块状态
                    self.update_block_status(block_id, True, remote_hash)
                    logger.info(f"区块 {block_height} 验证成功")

                except Exception as e:
                    logger.error(f"检查区块 {block_height} 时发生错误: {e}")
                    # 如果发生错误，将区块标记为无效
                    self.handle_invalid_block(block_id, block_height)
                    
            except Exception as e:
                logger.error(f"检查区块时出错 (尝试 {attempt + 1}/{self.max_retries}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise
            finally:
                if conn:
                    try:
                        db_pool.putconn(conn)
                    except Exception as e:
                        logger.error(f"释放数据库连接时出错: {str(e)}")

    def handle_invalid_block(self, block_id, block_height):
        """处理无效区块"""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                # 开始事务
                cur.execute("BEGIN")
            
                try:
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
                        username, reward_amount, reward_type = reward
                        if reward_type == 'tari':
                            cur.execute("""
                                UPDATE account 
                                SET tari_balance = tari_balance - %s
                                WHERE username = %s
                            """, (reward_amount, username))
                        elif reward_type == 'xmr':
                            cur.execute("""
                                UPDATE account 
                                SET xmr_balance = xmr_balance - %s
                                WHERE username = %s
                            """, (reward_amount, username))
                    
                    # 4. 删除奖励记录
                    cur.execute("""
                        UPDATE rewards 
                        SET reward = 0
                        WHERE block_height = %s
                    """, (block_height,))
                    
                    # 提交事务
                    cur.execute("COMMIT")
                    logger.info(f"区块 {block_height} 已标记为无效并清理相关数据")
                
                except Exception as e:
                    # 回滚事务
                    cur.execute("ROLLBACK")
                    raise e
                
        except Exception as e:
            logger.error(f"处理无效区块 {block_height} 时发生错误: {e}")
        finally:
            if conn:
                try:
                    db_pool.putconn(conn)
                except Exception as e:
                    logger.error(f"释放数据库连接时出错: {str(e)}")

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

def init_db():
    """初始化数据库连接池"""
    global db_pool
    try:
        db_pool = pool.ThreadedConnectionPool(
            minconn=5,  # 增加最小连接数
            maxconn=20,  # 增加最大连接数
            **DB_CONFIG,
            name='monitor_pool'
        )
        logger.info("Successfully connected to database")
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def get_db_connection():
    """获取数据库连接，带重试机制"""
    max_retries = 3
    retry_delay = 1  # 秒
    
    for attempt in range(max_retries):
        try:
            conn = db_pool.getconn()
            return conn
        except pool.PoolError as e:
            if attempt < max_retries - 1:
                logger.warning(f"获取数据库连接失败，尝试重试 ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                logger.error("数据库连接池耗尽，无法获取连接")
                raise
        except Exception as e:
            logger.error(f"获取数据库连接时发生错误: {str(e)}")
            raise

def handle_xmr_block(block_data):
    """处理 XMR 区块数据"""
    conn = None
    try:
        conn = get_db_connection()  # 使用新的连接获取函数
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
            try:
                db_pool.putconn(conn)
            except Exception as e:
                logger.error(f"释放数据库连接时出错: {str(e)}")

def handle_tari_block(block_data):
    """处理 TARI 区块数据"""
    conn = None
    try:
        conn = get_db_connection()  # 使用新的连接获取函数
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
            try:
                db_pool.putconn(conn)
            except Exception as e:
                logger.error(f"释放数据库连接时出错: {str(e)}")

def main():
    """主函数"""
    log_monitor = None
    block_checker = None
    
    try:
        # 初始化数据库连接
        init_db()
        logger.info("数据库连接初始化成功")
        
        # 创建并启动日志监控线程
        log_monitor = LogMonitorThread()
        log_monitor.start()
        logger.info("日志监控线程已启动")
        
        # 创建并启动区块检查线程
        block_checker = TariBlockChecker()
        block_checker.start()
        logger.info("区块检查线程已启动")
        
        # 保持程序运行
        while True:
            try:
                # 检查线程是否还在运行
                if not log_monitor.is_alive():
                    logger.error("日志监控线程已停止，重新启动...")
                    log_monitor = LogMonitorThread()
                    log_monitor.start()
                
                if not block_checker.is_alive():
                    logger.error("区块检查线程已停止，重新启动...")
                    block_checker = TariBlockChecker()
                    block_checker.start()
                
                time.sleep(10)  # 每10秒检查一次线程状态
                
            except Exception as e:
                logger.error(f"主循环发生错误: {str(e)}")
                time.sleep(5)  # 发生错误时等待5秒后继续
                
    except KeyboardInterrupt:
        logger.info("收到终止信号，正在关闭程序...")
    except Exception as e:
        logger.error(f"程序运行错误: {str(e)}")
    finally:
        # 确保正确关闭所有资源
        if log_monitor:
            log_monitor.stop()
            log_monitor.join(timeout=5)
            logger.info("日志监控线程已停止")
            
        if block_checker:
            block_checker.stop()
            block_checker.join(timeout=5)
            logger.info("区块检查线程已停止")
            
        if db_pool:
            db_pool.closeall()
            logger.info("数据库连接池已关闭")

if __name__ == "__main__":
    # 创建日志记录器
    logger = logging.getLogger('monitor')
    logger.setLevel(logging.INFO)
    
    # 创建文件处理器
    file_handler = logging.FileHandler('monitor.log')
    file_handler.setLevel(logging.INFO)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 启动主程序
    main()
