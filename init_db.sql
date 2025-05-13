-- 创建数据库
CREATE DATABASE p2pool;

-- 连接到数据库
\c p2pool;

-- 创建用户账户表
CREATE TABLE account (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    xmr_balance DECIMAL(20, 12) DEFAULT 0,
    tari_balance DECIMAL(20, 12) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建区块记录表
CREATE TABLE blocks (
    block_height BIGINT PRIMARY KEY,
    rewards DECIMAL(20, 12) NOT NULL,
    type VARCHAR(10) NOT NULL CHECK (type IN ('xmr', 'tari')),
    time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_shares BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建收益记录表
CREATE TABLE rewards (
    id SERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    type VARCHAR(10) NOT NULL CHECK (type IN ('xmr', 'tari')),
    username VARCHAR(255) NOT NULL,
    reward DECIMAL(20, 12) NOT NULL,
    shares BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (block_height) REFERENCES blocks(block_height),
    FOREIGN KEY (username) REFERENCES account(username)
);

-- 创建支付记录表
CREATE TABLE payment (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    type VARCHAR(10) NOT NULL CHECK (type IN ('xmr', 'tari')),
    amount DECIMAL(20, 12) NOT NULL,
    txid VARCHAR(255) NOT NULL,
    time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (username) REFERENCES account(username)
);

-- 创建索引
CREATE INDEX idx_blocks_type ON blocks(type);
CREATE INDEX idx_blocks_time ON blocks(time);
CREATE INDEX idx_rewards_block_height ON rewards(block_height);
CREATE INDEX idx_rewards_username ON rewards(username);
CREATE INDEX idx_payment_username ON payment(username);
CREATE INDEX idx_payment_time ON payment(time);

-- 创建更新时间触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为account表添加更新时间触发器
CREATE TRIGGER update_account_updated_at
    BEFORE UPDATE ON account
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column(); 