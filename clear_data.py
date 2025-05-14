#!/usr/bin/env python3
import redis
import psycopg2
import json
import logging
import sys
from datetime import datetime
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Redis配置
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# 加载数据库配置
def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        sys.exit(1)

def clear_redis():
    """清空Redis数据"""
    try:
        # 连接Redis
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        
        # 获取所有键
        xmr_keys = redis_client.keys('xmr:submit:*')
        tari_keys = redis_client.keys('tari:submit:*')
        
        # 删除所有键
        if xmr_keys:
            redis_client.delete(*xmr_keys)
            logger.info(f"已删除 {len(xmr_keys)} 个 XMR 提交记录")
            
        if tari_keys:
            redis_client.delete(*tari_keys)
            logger.info(f"已删除 {len(tari_keys)} 个 TARI 提交记录")
            
        logger.info("Redis 数据清理完成")
        
    except Exception as e:
        logger.error(f"清理 Redis 数据时出错: {str(e)}")
        sys.exit(1)

def clear_database():
    """清空数据库数据"""
    config = load_config()
    try:
        # 连接数据库
        conn = psycopg2.connect(
            host=config['database']['host'],
            port=config['database']['port'],
            database=config['database']['database'],
            user=config['database']['user'],
            password=config['database']['password']
        )
        cur = conn.cursor()
        
        try:
            # 开始事务
            cur.execute("BEGIN")
            
            # 清空 rewards 表
            cur.execute("TRUNCATE TABLE rewards CASCADE")
            logger.info("已清空 rewards 表")
            
            # 清空 blocks 表
            cur.execute("TRUNCATE TABLE blocks CASCADE")
            logger.info("已清空 blocks 表")
            
            # 重置 account 表中的余额
            cur.execute("""
                UPDATE account 
                SET xmr_balance = 0, 
                    tari_balance = 0
            """)
            logger.info("已重置所有账户余额")
            
            # 提交事务
            conn.commit()
            logger.info("数据库数据清理完成")
            
        except Exception as e:
            conn.rollback()
            raise e
            
        finally:
            cur.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"清理数据库数据时出错: {str(e)}")
        sys.exit(1)

def backup_database():
    """备份数据库数据"""
    config = load_config()
    try:
        # 创建备份目录
        backup_dir = 'db_backup'
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
            logger.info(f"创建备份目录: {backup_dir}")
        
        # 生成备份文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(backup_dir, f'database_backup_{timestamp}.sql')
        
        # 执行备份命令
        backup_cmd = f"pg_dump -h {config['database']['host']} -p {config['database']['port']} -U {config['database']['user']} -d {config['database']['database']} > {backup_file}"
        
        logger.info(f"开始备份数据库到文件: {backup_file}")
        os.system(backup_cmd)
        logger.info("数据库备份完成")
        
    except Exception as e:
        logger.error(f"备份数据库时出错: {str(e)}")
        sys.exit(1)

def main():
    """主函数"""
    print("警告：此操作将清空所有 Redis 和数据库数据！")
    print("建议在执行前先备份数据库。")
    choice = input("是否继续？(y/N): ").lower()
    
    if choice != 'y':
        print("操作已取消")
        sys.exit(0)
        
    # 备份数据库
    backup_choice = input("是否要备份数据库？(Y/n): ").lower()
    if backup_choice != 'n':
        backup_database()
    
    # 清空数据
    clear_redis()
    clear_database()
    
    print("所有数据清理完成！")

if __name__ == '__main__':
    main() 