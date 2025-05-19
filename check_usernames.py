import json
import psycopg2
from psycopg2 import Error

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

def check_username(connection, username):
    try:
        username ="%"+username
        cursor = connection.cursor()
        query = "SELECT username FROM ACCOUNT WHERE username like %s"
        cursor.execute(query, (username,))
        result = cursor.fetchone()
        cursor.close()
        return result
    except Error as e:
        print(f"Error checking username: {e}")
        return None

def main():
    # Read database configuration
    db_config = read_config()
    
    # Connect to database
    connection = connect_to_db(db_config)
    if not connection:
        return
    
    try:
        # Read usernames from 1.txt
        with open('1.txt', 'r') as f:
            for line in f:
                username = line.strip()
                if username:  # Skip empty lines
                    result = check_username(connection, username)
                    if result:                        
                        # 打印每个字段的值
                        print(f"{username}: {result[0]}")
                    else:
                        print(f"{username}: notfound")
    
    except FileNotFoundError:
        print("Error: 1.txt file not found")
    except Error as e:
        print(f"Database error: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main() 