import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import psycopg2
import hashlib
from groq import Groq

# Load environment variables from .env file (in the same folder)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# API configurations
API_KEY_NEWS = os.getenv('API_KEY_NEWS')
API_GROQ = os.getenv('API_GROQ')

# List of companies to search
companies = ["Apple", "Meta", "Nvidia", "Netflix"]

# Groq client configuration
groq_client = Groq(api_key=API_GROQ)

def generate_hash(company: str, title: str, published_at: str) -> str:
    """
    Generates a unique hash based on company, title and publication date
    """
    hash_string = f"{company}_{title}_{published_at}"
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

def analyze_news_sentiment(news_text):
    """
    Analyzes a single news item and returns 'good', 'bad' or 'neutral'
    """
    try:
        response = groq_client.chat.completions.create(
            model="openai/gpt-oss-20b",
            messages=[
                {
                    "role": "user",
                    "content": f'Is the following news headline good, bad orneutral? Headline: {news_text}. Only answer with "good", "bad" or neutral.'
                }
            ],
            temperature=0.0
        )
        return response.choices[0].message.content.strip().lower()
    except Exception as e:
        print(f"Error analyzing sentiment: {e}")
        return "neutral"

def get_news_data(url, query, api_key):
    """
    Fetches news for a specific company
    """
    params = {
        'q': query,
        'apiKey': api_key,
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': 30,
        'domains': 'bloomberg.com,reuters.com,cnbc.com,techcrunch.com',
        'from': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    }
    
    try:
        response = requests.get(url, params=params)
        
        print(f"\n{'='*60}")
        print(f"Fetching news for: {query}")
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('status') == 'ok' and 'articles' in data:
                articles = data['articles']
                print(f"Total articles found: {len(articles)}")
                
                if articles:
                    df = pd.DataFrame(articles)
                    df['company'] = query
                    return df[['url', 'company', 'publishedAt', 'title', 'description']]
                else:
                    print("No articles found for this company")
                    return pd.DataFrame()
                    
            else:
                print("API response error:")
                print(data)
                return pd.DataFrame()
                
        elif response.status_code == 401:
            print("ERROR - Authentication failed - Check your API Key")
            return pd.DataFrame()
        elif response.status_code == 429:
            print("ERROR - Rate limit exceeded - Please wait a moment")
            return pd.DataFrame()
        else:
            print(f"HTTP ERROR {response.status_code}")
            print(response.text)
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Connection ERROR: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected ERROR: {e}")
        return pd.DataFrame()

def insert_news_data(df: pd.DataFrame):
    """
    Inserts news data into PostgreSQL database with duplicate checking
    """
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Prepare data for insertion
        insert_data = []
        for _, row in df.iterrows():
            hash_key = generate_hash(row['company'], row['title'], str(row['publishedAt']))
            insert_data.append((
                hash_key,
                row['company'],
                row['title'],
                row['description'],
                row['url'],
                row['publishedAt'],
                row['sentiment']
            ))
        
        # Query to insert with ON CONFLICT (avoids duplicates)
        insert_query = """
        INSERT INTO bronze.news (hash, company, title, description, url, published_at, sentiment)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (hash) DO NOTHING
        """
        
        cursor.executemany(insert_query, insert_data)
        connection.commit()
        
        print(f"News data inserted into database successfully! {len(insert_data)} records processed.")
        return True
        
    except Exception as e:
        print(f"Error inserting data into database: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def create_news_table():
    """
    Creates the news table in PostgreSQL database if it doesn't exist
    """
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS bronze.news (
            hash VARCHAR(32) PRIMARY KEY,
            company VARCHAR(100) NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            url TEXT,
            published_at TIMESTAMP,
            sentiment VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        print("Table bronze.news created/verified successfully!")
        return True
        
    except Exception as e:
        print(f"Error creating table: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def test_db_connection():
    """
    Tests database connection before starting processing
    """
    print("\n" + "="*60)
    print("TESTING DATABASE CONNECTION...")
    print("="*60)
    
    connection = get_db_connection()
    if connection is None:
        print("ERROR - FAILED: Could not connect to database")
        print("Check your credentials in the .env file")
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"OK - Connection established successfully!")
        print(f"  PostgreSQL version: {version[0]}")
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"ERROR testing connection: {e}")
        if connection:
            connection.close()
        return False

def main():
    """
    Main function that processes all companies: collects news, analyzes sentiment and saves to database
    """
    # Test database connection BEFORE starting processing
    if not test_db_connection():
        print("\nINTERRUPTING: Script will not run due to database connection failure")
        return
    
    url = 'https://newsapi.org/v2/everything'
    
    # Create table if it doesn't exist
    print("\nChecking/creating table in database...")
    create_news_table()
    
    all_data = []
    
    for i, company in enumerate(companies):
        # Fetch company data
        df = get_news_data(url, company, API_KEY_NEWS)
        
        if not df.empty:
            print(f"OK - Data collected for {company} ({len(df)} articles)")
            
            # Analyze sentiment for each news item
            print(f"Analyzing sentiment for {company}...")
            sentiments = []
            
            for index, row in df.iterrows():
                print(f"  Processing news {index + 1}/{len(df)}: {row['title'][:50]}...")
                sentiment = analyze_news_sentiment(row['description'])
                sentiments.append(sentiment)
                
                # Small delay to avoid Groq API rate limit
                time.sleep(0.5)
            
            # Add sentiment column
            df['sentiment'] = sentiments
            
            all_data.append(df)
            print(f"OK - Sentiment analysis for {company} completed")
        
        # Delay between requests to avoid rate limit (except for the last one)
        if i < len(companies) - 1:
            print("Waiting 2 seconds before next request...")
            time.sleep(2)
    
    # Concatenate all DataFrames
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Save to database
        print("\nSaving data to PostgreSQL database...")
        success = insert_news_data(final_df)
        
        if success:
            print(f"\n{'='*60}")
            print(f"OK - Process completed!")
            print(f"Total articles processed: {len(final_df)}")
            print(f"Data saved to PostgreSQL database!")
            print(f"\nSummary by company:")
            print(final_df['company'].value_counts())
            print(f"\nSentiment distribution:")
            print(final_df['sentiment'].value_counts())
        else:
            print("Error saving to database. Saving as backup CSV...")
            final_df.to_csv('raw_data/news_data_with_sentiment_backup.csv', index=False)
            print(f"Data saved to: news_data_with_sentiment_backup.csv")
    else:
        print("\nERROR - No data was collected")

if __name__ == "__main__":
    main()
