import yfinance as yf
import pandas as pd
import psycopg2
import hashlib
import os
from dotenv import load_dotenv

# Load environment variables (from the same folder)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

def generate_hash(ticket: str, date: str) -> str:
    """
    Generates a unique hash based on ticket and date
    """
    hash_string = f"{ticket}_{date}"
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
            user=os.getenv('DATABASE_USER', 'postgres'),  # Assuming default user if not specified
            password=os.getenv('DATABASE_PASS'),
            sslmode='require'
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def insert_stock_data(df: pd.DataFrame):
    """
    Inserts data into PostgreSQL database with duplicate checking
    """
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Prepare data for insertion
        insert_data = []
        for _, row in df.iterrows():
            hash_key = generate_hash(row['Ticket'], str(row['Date']))
            insert_data.append((
                hash_key,
                row['Ticket'],
                row['Date'],
                row['Close'],
                row['High'],
                row['Low'],
                row['Open'],
                row['Volume']
            ))
        
        # Query to insert with ON CONFLICT (avoids duplicates)
        insert_query = """
        INSERT INTO bronze.stocks (hash, Ticket, Date, Close, High, Low, Open, Volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (hash) DO NOTHING
        """
        
        cursor.executemany(insert_query, insert_data)
        connection.commit()
        
        print(f"Data inserted into database successfully! {len(insert_data)} records processed.")
        return True
        
    except Exception as e:
        print(f"Error inserting data into database: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def get_multiple_stocks(tickets: list, period: str, save_to_db: bool = True, filename: str = "raw_data/stock_data.csv"):
    all_data = []
    
    for ticket in tickets:
        print(f"Downloading data for {ticket}...")
        data = yf.download(ticket, period=period, progress=False)
        
        # Remove MultiIndex if exists (flattens columns)
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        # Reset index to transform Date into a column
        data = data.reset_index()
        
        # Add Ticket column
        data['Ticket'] = ticket
        
        all_data.append(data)
    
    # Combine all DataFrames
    df_combined = pd.concat(all_data, ignore_index=True)
    
    # Reorganize columns: Ticket and Date first
    cols = ['Ticket', 'Date'] + [col for col in df_combined.columns if col not in ['Ticket', 'Date']]
    df_combined = df_combined[cols]
    
    if save_to_db:
        # Save to database
        success = insert_stock_data(df_combined)
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


# Usage
tickets = ["AAPL", "META", "NVDA", "NFLX"]
df = get_multiple_stocks(tickets, period="1d", save_to_db=True)
