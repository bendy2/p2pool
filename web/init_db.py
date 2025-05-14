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

    # 创建索引
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocks_chain_type ON blocks(chain_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp)')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("数据库初始化完成") 