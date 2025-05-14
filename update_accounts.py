import redis
import psycopg2
import json
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('update_accounts.log')
    ]
)

logger = logging.getLogger(__name__)

# Redis连接配置
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# Redis键前缀
XMR_PREFIX = "xmr:submit:"
TARI_PREFIX = "tari:submit:"

# 加载配置文件
def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"加载配置文件失败: {str(e)}")
        raise

config = load_config()

# PostgreSQL数据库连接
def get_db_connection():
    return psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['database'],
        user=config['database']['user'],
        password=config['database']['password']
    )

def load_users_from_file(filename):
    """从文件加载用户数据"""
    users = set()
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:  # 跳过空行
                    continue
                try:
                    # 分割行数据
                    parts = line.split('\t')
                    if len(parts) >= 3:
                        username = parts[0].strip()
                        xmr_wallet = parts[1].strip()
                        tari_wallet = parts[2].strip()
                        users.add((username, xmr_wallet, tari_wallet))
                except Exception as e:
                    logger.warning(f"解析行数据失败: {line}, 错误: {str(e)}")
                    continue
    except Exception as e:
        logger.error(f"读取文件失败: {str(e)}")
    return users

def update_accounts():
    """从Redis获取用户列表并更新account表"""
    try:
        # 连接Redis
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        
        # 连接数据库
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 获取所有用户
        users = set()
        
        # 从XMR提交记录中获取用户
        for key in redis_client.keys(f"{XMR_PREFIX}*"):
            data = key.replace(XMR_PREFIX, '')
            try:
                username = data.split(':')[1]
                xmr_wallet = data.split(':')[0]
                tari_wallet = data.split(':')[1]
                users.add((username, xmr_wallet, tari_wallet))
            except IndexError:
                logger.warning(f"无法解析用户名格式: {data}")
                continue
        
        # 从TARI提交记录中获取用户
        for key in redis_client.keys(f"{TARI_PREFIX}*"):
            data = key.replace(TARI_PREFIX, '')
            try:
                username = data.split(':')[1]
                xmr_wallet = data.split(':')[0]
                tari_wallet = data.split(':')[1]
                users.add((username, xmr_wallet, tari_wallet))
            except IndexError:
                logger.warning(f"无法解析用户名格式: {data}")
                continue
        
        # 从文件加载用户数据
        file_users = load_users_from_file('users.txt')
        users.update(file_users)
        
        # 更新数据库
        updated_count = 0
        for username, xmr_wallet, tari_wallet in users:
            try:
                # 检查用户是否存在
                cur.execute("""
                    SELECT COUNT(*) FROM account WHERE username = %s
                """, (username,))
                
                if cur.fetchone()[0] == 0:
                    # 用户不存在，创建新用户
                    cur.execute("""
                        INSERT INTO account (username, xmr_wallet, tari_wallet, xmr_balance, tari_balance, fee)
                        VALUES (%s, %s, %s, 0, 0, %s)
                    """, (username, xmr_wallet, tari_wallet, config['pool_fees']))
                    logger.info(f"创建新用户: {username}")
                else:
                    # 用户存在，更新钱包地址
                    cur.execute("""
                        UPDATE account 
                        SET xmr_wallet = %s, tari_wallet = %s
                        WHERE username = %s
                    """, (xmr_wallet, tari_wallet, username))
                    logger.info(f"更新用户钱包: {username}")
                
                updated_count += 1
                
            except Exception as e:
                logger.error(f"更新用户 {username} 失败: {str(e)}")
                continue
        
        conn.commit()
        logger.info(f"更新完成，共处理 {updated_count} 个用户")
        
    except Exception as e:
        logger.error(f"更新过程发生错误: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    logger.info("开始更新用户账户...")
    update_accounts()
    logger.info("更新完成") 