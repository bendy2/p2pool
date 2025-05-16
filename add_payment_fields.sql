-- 添加status字段，允许为空
ALTER TABLE payment ADD COLUMN status VARCHAR(20) CHECK (status IN ('pending', 'completed', 'failed'));

-- 添加note字段，允许为空
ALTER TABLE payment ADD COLUMN note TEXT;

-- 为status字段创建索引
CREATE INDEX idx_payment_status ON payment(status);

-- 更新现有记录的状态为completed
UPDATE payment SET status = 'completed' WHERE status IS NULL; 