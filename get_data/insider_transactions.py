import requests
import pandas as pd
import psycopg2
import hashlib
import os
from dotenv import load_dotenv
from datetime import date, timedelta

# Load environment variables (from the same folder)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

def generate_hash(symbol: str, name: str, transaction_date: str, change: int, share: int) -> str:
    """
    Generates a unique hash based on symbol, name, transaction date, change and share
    """
    hash_string = f"{symbol}_{name}_{transaction_date}_{change}_{share}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def get_db_connection():
    """
    Establishes connection with PostgreSQL database
    """
    try:
        connection = psycopg2.connect(
            host=os.getenv('HOSTNAME'),
            port=os.getenv('PORT'),
            database=os.getenv('DATABASE'),
            user=os.getenv('DATABASE_USER', 'postgres'),
            password=os.getenv('DATABASE_PASS'),
            sslmode='require'
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def insert_insider_data(df: pd.DataFrame):
    """
    Inserts insider transaction data into PostgreSQL database with duplicate checking
    """
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.insider_transactions (
            hash VARCHAR(32) PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            name TEXT,
            share BIGINT,
            change BIGINT,
            filing_date DATE,
            transaction_date DATE,
            transaction_price NUMERIC(10, 2),
            transaction_code VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Prepare data for insertion
        insert_data = []
        for _, row in df.iterrows():
            hash_key = generate_hash(
                row['symbol'], 
                row['name'], 
                row['transactionDate'], 
                row['change'],
                row['share']
            )
            
            # Handle transaction_date - API returns date string, not timestamp
            transaction_date = pd.to_datetime(row['transactionDate']) if pd.notna(row['transactionDate']) else None
            filing_date = pd.to_datetime(row['filingDate']) if pd.notna(row['filingDate']) else None
            
            insert_data.append((
                hash_key,
                row['symbol'],
                row['name'],
                row['share'],
                row['change'],
                filing_date,
                transaction_date,
                row['transactionPrice'],
                row['transactionCode']
            ))
        
        # Query to insert with ON CONFLICT (avoids duplicates)
        insert_query = """
        INSERT INTO bronze.insider_transactions (hash, symbol, name, share, change, filing_date, transaction_date, transaction_price, transaction_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (hash) DO NOTHING
        """
        
        cursor.executemany(insert_query, insert_data)
        connection.commit()
        
        print(f"Insider transaction data inserted into database successfully! {len(insert_data)} records processed.")
        return True
        
    except Exception as e:
        print(f"Error inserting data into database: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def get_insider_transactions(symbols: list, save_to_db: bool = True, filename: str = "raw_data/insider_transactions.csv"):
    all_data = []
    
    api_key = os.getenv('API_KEY_TRADEOFF')
    yesterday = date.today() - timedelta(days=1)
    date_filter_str = yesterday.strftime('%Y-%m-%d')
    for symbol in symbols:
        print(f"Downloading data for {symbol}...")
        url = 'https://finnhub.io/api/v1/stock/insider-transactions'
        params = {
            'symbol': symbol,
            'token': api_key,
            'from': date_filter_str,
            'to': date_filter_str
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                df['symbol'] = symbol
                all_data.append(df)
                print(f"  Found {len(df)} transactions")
        except Exception as e:
            print(f"  Error fetching data for {symbol}: {e}")
    
    # Combine all DataFrames
    if all_data:
        df_combined = pd.concat(all_data, ignore_index=True)
        
        if save_to_db:
            # Save to database
            success = insert_insider_data(df_combined)
            if success:
                print("Data saved to PostgreSQL database!")
            else:
                print("Error saving to database. Saving as CSV...")
                df_combined.to_csv(filename, index=False)
                print(f"Data saved to: {filename}")
        else:
            # Save as CSV (original behavior)
            df_combined.to_csv(filename, index=False)
            print(f"Data saved to: {filename}")
        
        return df_combined
    else:
        print("No data collected")
        return pd.DataFrame()

# Usage
symbols = ["AAPL", "META", "NVDA", "NFLX"]
df = get_insider_transactions(symbols, save_to_db=True)
