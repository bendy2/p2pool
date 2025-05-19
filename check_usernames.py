import json
import psycopg2
from psycopg2 import Error
from datetime import datetime, timedelta

def read_config():
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config['database']

def connect_to_db(db_config):
    try:
        connection = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        return connection
    except Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def find_account_by_tari_address(connection, tari_address):
    try:
        cursor = connection.cursor()
        query = "SELECT username, xmr_balance, tari_balance FROM ACCOUNT WHERE tari_wallet = %s"
        cursor.execute(query, (tari_address,))
        result = cursor.fetchone()
        cursor.close()
        return result
    except Error as e:
        print(f"Error finding account: {e}")
        return None

def add_reward(connection, username, amount, currency, block_height):
    try:
        cursor = connection.cursor()
        created_at = datetime.now() - timedelta(days=2)
        query = """
            INSERT INTO rewards (username, amount, currency, block_height, shares, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (username, amount, currency, block_height, 0, created_at))
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        print(f"Error adding reward: {e}")
        return False

def update_account_balance(connection, username, xmr_amount, tari_amount):
    try:
        cursor = connection.cursor()
        query = """
            UPDATE ACCOUNT 
            SET xmr_balance = xmr_balance + %s,
                tari_balance = tari_balance + %s
            WHERE username = %s
        """
        cursor.execute(query, (xmr_amount, tari_amount, username))
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        print(f"Error updating balance: {e}")
        return False

def main():
    # Read database configuration
    db_config = read_config()
    
    # Connect to database
    connection = connect_to_db(db_config)
    if not connection:
        return
    
    try:
        # Read data from 1.txt
        with open('1.txt', 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 3:
                    tari_address, xmr_amount, tari_amount = parts
                    xmr_amount = float(xmr_amount)
                    tari_amount = float(tari_amount)
                    
                    # Find account by TARI address
                    account = find_account_by_tari_address(connection, tari_address)
                    
                    if account:
                        username, current_xmr, current_tari = account
                        print(f"找到用户: {username}")
                        
                        # Add rewards
                        if add_reward(connection, username, xmr_amount, 'XMR', 1):
                            print(f"已添加 XMR 奖励: {xmr_amount}")
                        if add_reward(connection, username, tari_amount, 'TARI', 2):
                            print(f"已添加 TARI 奖励: {tari_amount}")
                        
                        # Update account balance
                        if update_account_balance(connection, username, xmr_amount, tari_amount):
                            print(f"已更新账户余额")
                            print(f"XMR余额: {current_xmr + xmr_amount}")
                            print(f"TARI余额: {current_tari + tari_amount}")
                    else:
                        print(f"未找到TARI地址对应的账户: {tari_address}")
    
    except FileNotFoundError:
        print("Error: 1.txt file not found")
    except Error as e:
        print(f"Database error: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main() 