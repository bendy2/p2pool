#!/usr/bin/env python3
import json
import logging
import psycopg2
import requests
from datetime import datetime
from decimal import Decimal

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('restore_tari_block.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TariBlockRestorer:
    def __init__(self):
        self.config = self.load_config()
        self.init_database()
        self.api_url = "https://textexplore.tari.com/blocks/{height}?json"

    def load_config(self):
        """加载配置文件"""
        try:
            with open('../config.json', 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

    def init_database(self):
        """初始化数据库连接"""
        try:
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

    def buffer_to_hex(self, buffer_data):
        """将 Buffer 数据转换为十六进制字符串"""
        if not isinstance(buffer_data, dict) or 'data' not in buffer_data:
            return ''
        return ''.join([f'{x:02x}' for x in buffer_data['data']])

    def get_block_from_api(self, height):
        """从 API 获取区块数据"""
        try:
            url = self.api_url.format(height=height)
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            if 'application/json' not in response.headers.get('content-type', ''):
                logger.warning(f"API 响应不是 JSON 格式")
                return None
                
            data = response.json()
            if not data:
                logger.warning(f"API 返回空数据")
                return None
            return data
                
        except Exception as e:
            logger.error(f"获取区块 {height} 数据失败: {e}")
            return None

    def get_reference_block_shares(self, reference_height):
        """获取参考区块的用户份额分布"""
        try:
            self.cursor.execute("""
                SELECT username, shares
                FROM rewards 
                WHERE block_height = %s 
                AND type = 'tari'
            """, (reference_height,))
            
            shares_data = self.cursor.fetchall()
            if not shares_data:
                logger.error(f"未找到参考区块 {reference_height} 的份额数据")
                return None

            # 计算总份额
            total_shares = sum(shares for _, shares in shares_data)
            
            # 计算每个用户的份额比例
            share_ratios = {
                username: Decimal(str(shares)) / Decimal(str(total_shares))
                for username, shares in shares_data
            }
            
            return share_ratios, total_shares

        except Exception as e:
            logger.error(f"获取参考区块份额数据失败: {e}")
            return None

    def restore_block(self, block_height, reference_height):
        """恢复指定区块的奖励"""
        try:
            # 1. 获取区块信息
            self.cursor.execute("""
                SELECT block_height, rewards, total_shares
                FROM blocks 
                WHERE block_height = %s 
                AND type = 'tari'
            """, (block_height,))
            
            block = self.cursor.fetchone()
            if not block:
                logger.error(f"未找到区块 {block_height}")
                return False

            block_height, rewards, total_shares = block

            # 2. 从API获取区块数据
            api_data = self.get_block_from_api(block_height)
            if not api_data:
                logger.error(f"无法从API获取区块 {block_height} 数据")
                return False

            # 3. 验证区块哈希
            header = api_data.get('header', {})
            remote_hash = self.buffer_to_hex(header.get('hash', {}))
            
            if not remote_hash:
                logger.error(f"区块 {block_height} 未找到远程哈希")
                return False

            # 4. 获取参考区块的份额分布
            reference_data = self.get_reference_block_shares(reference_height)
            if not reference_data:
                return False

            share_ratios, ref_total_shares = reference_data

            # 5. 开始恢复过程
            self.cursor.execute("BEGIN")

            # 6. 计算并恢复用户奖励
            for username, ratio in share_ratios.items():
                # 计算用户份额
                user_shares = int(total_shares * ratio)
                
                # 计算用户奖励
                user_reward = Decimal(str(rewards)) * ratio
                
                # 插入奖励记录
                self.cursor.execute("""
                    INSERT INTO rewards (
                        block_height, type, username, reward, shares, time
                    ) VALUES (
                        %s, 'tari', %s, %s, %s, CURRENT_TIMESTAMP
                    )
                """, (block_height, username, user_reward, user_shares))
                
                # 更新用户余额
                self.cursor.execute("""
                    UPDATE account 
                    SET tari_balance = tari_balance + %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE username = %s
                """, (user_reward, username))
                
                logger.info(f"已恢复用户 {username} 的奖励: {user_reward} TARI (份额: {user_shares})")

            # 7. 提交事务
            self.conn.commit()
            logger.info(f"区块 {block_height} 恢复成功")
            return True

        except Exception as e:
            self.conn.rollback()
            logger.error(f"恢复区块 {block_height} 时发生错误: {e}")
            return False

    def restore_blocks(self, block_heights, reference_height):
        """恢复多个区块的奖励"""
        success_count = 0
        fail_count = 0
        
        for height in block_heights:
            logger.info(f"开始恢复区块 {height}")
            if self.restore_block(height, reference_height):
                success_count += 1
            else:
                fail_count += 1
        
        logger.info(f"恢复完成: 成功 {success_count} 个区块, 失败 {fail_count} 个区块")
        return success_count, fail_count

def main():
    try:
        # 创建恢复器实例
        restorer = TariBlockRestorer()
        
        # 从命令行获取区块高度列表和参考区块高度
        import sys
        if len(sys.argv) < 3:
            print("使用方法: python3 restore_tari_block.py <reference_height> <block_height1> [block_height2 ...]")
            sys.exit(1)
        
        # 获取参考区块高度
        reference_height = int(sys.argv[1])
        
        # 转换区块高度为整数列表
        block_heights = [int(height) for height in sys.argv[2:]]
        
        # 执行恢复
        success, fail = restorer.restore_blocks(block_heights, reference_height)
        
        print(f"\n恢复结果:")
        print(f"参考区块: {reference_height}")
        print(f"成功: {success} 个区块")
        print(f"失败: {fail} 个区块")
        
    except Exception as e:
        logger.error(f"程序运行失败: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 