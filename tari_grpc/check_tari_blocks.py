#!/usr/bin/env python3
import json
import logging
import psycopg2
import requests
from datetime import datetime
from decimal import Decimal
from tabulate import tabulate

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('check_tari_blocks.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TariBlockChecker:
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

    def get_all_tari_blocks(self):
        """获取所有TARI区块"""
        try:
            self.cursor.execute("""
                SELECT block_height, rewards, total_shares, time,block_id
                FROM blocks 
                WHERE type = 'tari' and is_valid = false
                ORDER BY block_height DESC
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"获取区块列表失败: {e}")
            return []

    def check_block(self, block):
        """检查单个区块的有效性"""
        block_height = block[0]
        block_id = block[4]
        try:
            # 从API获取区块数据
            api_data = self.get_block_from_api(block_height)
            if not api_data:
                return {
                    'height': block_height,
                    'status': 'NOT_FOUND',
                    'message': '区块未在区块链上找到'
                }

            # 获取区块信息
            header = api_data.get('header', {})
            if not header:
                return {
                    'height': block_height,
                    'status': 'INVALID',
                    'message': '区块数据不完整'
                }
            if block_id != self.buffer_to_hex(header.get('hash', {})):
                return {
                    'height': block_height,
                    'status': 'INVALID',
                    'message': '区块哈希不匹配'
                }

            # 获取区块时间
            try:
                timestamp = int(header.get('timestamp', 0))
                block_time = datetime.fromtimestamp(timestamp)
            except (ValueError, TypeError):
                block_time = 'N/A'

            return {
                'height': block_height,
                'status': 'VALID',
                'message': '区块有效',
                'block_time': block_time
            }

        except Exception as e:
            logger.error(f"检查区块 {block_height} 时发生错误: {e}")
            return {
                'height': block_height,
                'status': 'ERROR',
                'message': f'检查过程出错: {str(e)}'
            }

    def check_all_blocks(self):
        """检查所有区块"""
        blocks = self.get_all_tari_blocks()
        if not blocks:
            logger.info("数据库中没有TARI区块记录")
            return

        results = []
        total_blocks = len(blocks)
        valid_blocks = 0
        invalid_blocks = 0
        not_found_blocks = 0
        error_blocks = 0

        print(f"\n开始检查 {total_blocks} 个TARI区块...")
        
        for block in blocks:
            block_height = block[0]
            block_id = block[4]
            
            result = self.check_block(block)
            
            # 统计结果
            if result['status'] == 'VALID':
                valid_blocks += 1
            elif result['status'] == 'NOT_FOUND':
                not_found_blocks += 1
            elif result['status'] == 'INVALID':
                invalid_blocks += 1
            else:
                error_blocks += 1

            # 添加到结果列表
            results.append([
                block_height,   
                result['status'],
                result['message'],
                result.get('block_time', 'N/A')
            ])

        # 打印统计信息
        print("\n检查结果统计:")
        print(f"总区块数: {total_blocks}")
        print(f"有效区块: {valid_blocks}")
        print(f"未找到区块: {not_found_blocks}")
        print(f"无效区块: {invalid_blocks}")
        print(f"检查错误: {error_blocks}")

        # 打印详细结果表格
        print("\n详细检查结果:")
        headers = ['区块高度', '状态', '说明', '区块时间']
        print(tabulate(results, headers=headers, tablefmt='grid'))

def main():
    try:
        checker = TariBlockChecker()
        checker.check_all_blocks()
    except Exception as e:
        logger.error(f"程序运行失败: {str(e)}")
        raise

if __name__ == "__main__":
    main() 