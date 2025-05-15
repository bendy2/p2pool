import json
import logging

logger = logging.getLogger(__name__)

def check_tari_blocks():
    try:
        # 读取stratum文件
        with open('./api/local/stratum', 'r') as f:
            data = json.load(f)
            
        # 获取区块信息
        blocks = data.get('blocks', [])
        if not blocks:
            return
            
        # 遍历区块
        for block in blocks:
            try:
                # 解析区块信息
                parts = block.split(':')
                if len(parts) < 5:
                    continue
                    
                # 检查parts[4]是否包含check_str
                check_str = "check"  # 这里可以根据需要修改检查的字符串
                if check_str in parts[4]:
                    logger.info(f"找到包含check_str的区块: {block}")
                    # 这里可以添加其他处理逻辑
                    
            except Exception as e:
                logger.error(f"处理区块信息失败: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"检查Tari区块失败: {str(e)}")

if __name__ == "__main__":
    check_tari_blocks() 