#!/usr/bin/env python3
import json
import logging
import psycopg2
import os
import csv
from datetime import datetime, timedelta
from decimal import Decimal

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tari_reward.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TariReward:
    def __init__(self):
        self.config = self.load_config()
        self.init_database()
        self.reward_date = datetime(2025, 5, 16)  # 2025年5月16日
        self.reward_percentage = Decimal('0.15')  # 15%奖励

    def load_config(self):
        """加载配置文件"""
        try:
            with open('../config.json', 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

    def backup_database(self):
        """使用Python备份数据库"""
        try:
            # 创建备份目录
            backup_dir = "database_backups"
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)

            # 生成备份文件名（使用时间戳）
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = os.path.join(backup_dir, f"payment_backup_{timestamp}.csv")

            # 需要备份的表
            tables = ['account', 'rewards', 'payment']

            # 创建备份连接
            backup_conn = psycopg2.connect(
                host=self.config['database']['host'],
                port=self.config['database']['port'],
                database=self.config['database']['database'],
                user=self.config['database']['user'],
                password=self.config['database']['password']
            )
            backup_cursor = backup_conn.cursor()

            # 开始备份
            logger.info("开始备份数据库...")
            
            with open(backup_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # 写入备份信息
                writer.writerow(['BACKUP_INFO'])
                writer.writerow(['timestamp', timestamp])
                writer.writerow(['database', self.config['database']['database']])
                writer.writerow([])  # 空行分隔

                # 备份每个表
                for table in tables:
                    # 获取表结构
                    backup_cursor.execute(f"""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (table,))
                    columns = backup_cursor.fetchall()
                    
                    # 写入表信息
                    writer.writerow(['TABLE_INFO'])
                    writer.writerow(['table_name', table])
                    writer.writerow(['columns'] + [col[0] for col in columns])
                    writer.writerow([])  # 空行分隔

                    # 获取表数据
                    backup_cursor.execute(f"SELECT * FROM {table}")
                    rows = backup_cursor.fetchall()
                    
                    # 写入数据
                    writer.writerow(['TABLE_DATA'])
                    for row in rows:
                        writer.writerow(row)
                    writer.writerow([])  # 空行分隔

            backup_cursor.close()
            backup_conn.close()

            logger.info(f"数据库备份成功: {backup_file}")
            return True

        except Exception as e:
            logger.error(f"数据库备份过程出错: {str(e)}")
            return False

    def init_database(self):
        """初始化数据库连接"""
        try:
            # 从配置文件获取数据库连接信息
            db_config = self.config.get('database', {})
            self.conn = psycopg2.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 5432),
                database=db_config.get('database', 'payment'),
                user=db_config.get('user', 'postgres'),
                password=db_config.get('password', '')
            )
            self.cursor = self.conn.cursor()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise

    def get_user_rewards(self):
        """获取用户5月16日的TARI奖励总和"""
        try:
            # 计算5月16日的开始和结束时间（北京时间）
            start_time = self.reward_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
            
            # 修改查询逻辑，处理用户名和钱包地址
            self.cursor.execute("""
                WITH parsed_rewards AS (
                    SELECT 
                        CASE 
                            WHEN LENGTH(username) > 50 AND username LIKE '%:%' THEN 
                                CASE 
                                    WHEN SPLIT_PART(username, ':', 2) = '' THEN username
                                    ELSE SPLIT_PART(username, ':', 2)
                                END
                            ELSE username 
                        END as parsed_username,
                        SUM(reward) as total_reward
                    FROM rewards
                    WHERE type = 'tari'
                    AND time >= %s
                    AND time < %s
                    GROUP BY 
                        CASE 
                            WHEN LENGTH(username) > 50 AND username LIKE '%:%' THEN 
                                CASE 
                                    WHEN SPLIT_PART(username, ':', 2) = '' THEN username
                                    ELSE SPLIT_PART(username, ':', 2)
                                END
                            ELSE username 
                        END
                    HAVING SUM(reward) > 0
                )
                SELECT parsed_username, total_reward
                FROM parsed_rewards
                ORDER BY total_reward DESC
            """, (start_time, end_time))
            
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"获取用户奖励失败: {str(e)}")
            return []

    def distribute_rewards(self):
        """发放奖励"""
        try:
            # 首先备份数据库
            if not self.backup_database():
                logger.error("数据库备份失败，终止奖励发放")
                return

            # 获取用户奖励数据
            user_rewards = self.get_user_rewards()
            if not user_rewards:
                logger.info("没有找到符合条件的奖励数据")
                return

            # 显示奖励信息
            print("\n奖励发放列表:")
            print("-" * 60)
            print(f"{'用户名':<20} {'原始奖励(TARI)':<15} {'额外奖励(TARI)':<15}")
            print("-" * 60)
            
            total_original = Decimal('0')
            total_bonus = Decimal('0')
            
            # 开始事务
            self.cursor.execute("BEGIN")
            
            for username, original_reward in user_rewards:
                # 确保original_reward是Decimal类型
                original_reward = Decimal(str(original_reward))
                
                # 计算额外奖励
                bonus_reward = original_reward * self.reward_percentage
                # 保留6位小数
                bonus_reward = Decimal(str(int(bonus_reward * Decimal('1000000')) / Decimal('1000000')))
                
                # 插入奖励记录
                current_time = datetime.now()
                self.cursor.execute("""
                    INSERT INTO rewards (
                        block_height, type, username, reward, shares, time, height
                    ) VALUES (
                        0, 'tari', %s, %s, 0, %s, 0
                    )
                """, (username, bonus_reward, current_time))
                
                # 更新用户余额
                self.cursor.execute("""
                    UPDATE account 
                    SET tari_balance = tari_balance + %s 
                    WHERE username = %s
                """, (bonus_reward, username))
                
                # 显示信息
                print(f"{username:<20} {original_reward:<15.6f} {bonus_reward:<15.6f}")
                
                total_original += original_reward
                total_bonus += bonus_reward
            
            # 提交事务
            self.conn.commit()
            
            print("-" * 60)
            print(f"总计原始奖励: {total_original:.6f} TARI")
            print(f"总计额外奖励: {total_bonus:.6f} TARI")
            print(f"奖励用户数: {len(user_rewards)}")
            
            logger.info(f"奖励发放完成: {len(user_rewards)} 个用户")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"发放奖励失败: {str(e)}")
            raise
        finally:
            self.cursor.close()
            self.conn.close()

def main():
    try:
        reward = TariReward()
        reward.distribute_rewards()
    except Exception as e:
        logger.error(f"程序运行失败: {str(e)}")

if __name__ == "__main__":
    main() 