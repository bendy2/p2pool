import requests
import json
import base64
import argparse
from typing import Dict, Any

def buffer_to_hex(buffer_data: Dict[str, Any]) -> str:
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

def verify_block(block_height: int) -> bool:
    """验证区块的有效性"""
    block_data = get_block_data(block_height)
    if not block_data:
        return False

    try:
        # 获取区块头信息
        header = block_data.get('header', {})
        if not header:
            print("未找到区块头信息")
            return False

        # 获取并转换 hash
        block_hash = buffer_to_hex(header.get('hash', {}))
        if not block_hash:
            print("未找到有效的区块哈希")
            return False

        # 获取并转换 prev_hash
        prev_hash = buffer_to_hex(header.get('prev_hash', {}))
        if not prev_hash:
            print("未找到有效的前一个区块哈希")
            return False

        # 打印验证信息
        print(f"区块高度: {block_height}")
        print(f"区块哈希: {block_hash}")
        print(f"前一个区块哈希: {prev_hash}")
        print(f"时间戳: {header.get('timestamp', 'N/A')}")
        print(f"版本: {header.get('version', 'N/A')}")

        # 这里可以添加更多的验证逻辑
        # 例如：验证时间戳、版本号等

        return True

    except Exception as e:
        print(f"验证区块时发生错误: {e}")
        return False

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='验证 Tari 区块的有效性')
    parser.add_argument('block_height', type=int, help='要验证的区块高度')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    print(f"开始验证区块 {args.block_height}...")
    
    if verify_block(args.block_height):
        print("区块验证成功！")
    else:
        print("区块验证失败！")

if __name__ == "__main__":
    main() 