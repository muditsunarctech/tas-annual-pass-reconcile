"""
Initialize MySQL Database
Runs the schema initialization script using credentials from .env
"""

import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def init_database():
    print("🚀 Initializing MySQL Database...")
    
    # Get configuration
    config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', 3306)),
        'user': os.getenv('MYSQL_USER'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'database': os.getenv('MYSQL_DATABASE')
    }
    
    # Read schema file
    schema_path = os.path.join(os.path.dirname(__file__), '../config/mysql_schema.sql')
    with open(schema_path, 'r') as f:
        schema_sql = f.read()
    
    try:
        # Connect to MySQL (without database first to create it)
        conn = mysql.connector.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password']
        )
        cursor = conn.cursor()
        
        # Execute schema commands
        # Split by semicolon to execute one by one
        commands = schema_sql.split(';')
        
        for command in commands:
            command = command.strip()
            if command:
                try:
                    cursor.execute(command)
                except mysql.connector.Error as err:
                    if "database exists" in str(err).lower():
                        pass
                    else:
                        print(f"⚠️ Warning executing command: {err}")
                        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Database initialization complete!")
        return True
        
    except mysql.connector.Error as err:
        print(f"❌ Error initializing database: {err}")
        return False

if __name__ == "__main__":
    init_database()
