-- 为account表添加fee字段
ALTER TABLE account ADD COLUMN fee DECIMAL(5,4) DEFAULT 0.08;

-- 更新现有account记录的fee值
UPDATE account SET fee = 0.08 WHERE fee IS NULL;

-- 为blocks表添加value字段
ALTER TABLE blocks ADD COLUMN value DECIMAL(20,8);

-- 更新现有blocks记录的value值
UPDATE blocks b
SET value = (
    SELECT COALESCE(b.rewards / NULLIF(SUM(r.shares), 0), 0)
    FROM rewards r
    WHERE r.block_height = b.block_height
    GROUP BY r.block_height
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_blocks_value ON blocks(value);
CREATE INDEX IF NOT EXISTS idx_account_fee ON account(fee); 