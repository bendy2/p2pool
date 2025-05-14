import sqlite3

def init_db():
    conn = sqlite3.connect('pool_data.db')
    cursor = conn.cursor()

    # 创建区块表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS blocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chain_type TEXT NOT NULL,
        height INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        reward REAL NOT NULL,
        hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 创建用户账户表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS account (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        balance REAL NOT NULL DEFAULT 0,
        total_rewards REAL NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 创建奖励表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reward (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        height INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        amount REAL NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (username) REFERENCES account(username)
    )
    ''')

    # 创建支付表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payment (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        txid TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        amount REAL NOT NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (username) REFERENCES account(username)
    )
    ''')

    # 创建索引
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocks_chain_type ON blocks(chain_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_reward_username ON reward(username)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_reward_timestamp ON reward(timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_payment_username ON payment(username)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_payment_timestamp ON payment(timestamp)')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("数据库初始化完成") 