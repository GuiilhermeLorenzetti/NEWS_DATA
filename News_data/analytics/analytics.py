import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv

# --- CONFIGURATION AND CONNECTION ---
def get_db_connection():
    """Connects to the database using environment variables."""
    load_dotenv() # Loads variables from .env file
    
    user = os.getenv("DATABASE_USER")
    password = os.getenv("DATABASE_PASS")
    host = os.getenv("HOSTNAME")
    port = os.getenv("PORT")
    dbname = os.getenv("DATABASE")
    
    connection_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    return create_engine(connection_str)

def load_data(engine):
    """Loads data from Gold tables."""
    print("--- Loading data from Data Warehouse ---")
    df_trans = pd.read_sql("SELECT * FROM gold.stock_transations", engine)
    df_news = pd.read_sql("SELECT * FROM gold.stock_news", engine)
    df_insider = pd.read_sql("SELECT * FROM gold.stock_insider_transations", engine)
    return df_trans, df_news, df_insider

# --- PROCESSING ---
def process_data(df_trans, df_news, df_insider):
    """Cleans and prepares data for analysis."""
    print("--- Processing data ---")
    
    # Convert dates
    df_trans['trading_date'] = pd.to_datetime(df_trans['trading_date'])
    df_news['news_date'] = pd.to_datetime(df_news['news_date'])
    df_insider['transaction_date'] = pd.to_datetime(df_insider['transaction_date'])
    
    # Standardize columns
    if 'symbol' in df_insider.columns:
        df_insider.rename(columns={'symbol': 'ticket'}, inplace=True)
        
    # Master Merge (Left Join on transactions to align dates)
    merged = pd.merge(df_trans, df_news, left_on=['ticket', 'trading_date'], right_on=['ticket', 'news_date'], how='left')
    merged = pd.merge(merged, df_insider, left_on=['ticket', 'trading_date'], right_on=['ticket', 'transaction_date'], how='left')
    
    # Fill nulls for numerical calculations
    merged['daily_sentiment_score'] = merged['daily_sentiment_score'].fillna(0)
    merged['net_value_flow'] = merged['net_value_flow'].fillna(0)
    
    # Create sentiment intensity metric (without negative sign)
    merged['sentiment_intensity'] = merged['daily_sentiment_score'].abs()
    
    return df_trans, df_news, df_insider, merged

# --- VISUALIZATIONS ---
def generate_visuals(df_trans, df_news, df_insider, merged_df):
    print("--- Generating Visualizations ---")
    
    # Define style
    sns.set_theme(style="whitegrid")
    
    # Choose a main ticker for specific analyses (e.g., META or the most frequent)
    target_ticker = 'META' 
    if target_ticker not in merged_df['ticket'].values:
        target_ticker = merged_df['ticket'].mode()[0]
        
    print(f"Focusing individual analyses on ticker: {target_ticker}")
    df_target = merged_df[merged_df['ticket'] == target_ticker].sort_values('trading_date')

    # 1. Price vs Sentiment (Dual Axis)
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.set_title(f'{target_ticker}: Sentiment Impact on Price')
    ax1.plot(df_target['trading_date'], df_target['close_price'], color='#1f77b4', linewidth=2, label='Closing Price')
    ax1.set_ylabel('Price ($)', color='#1f77b4', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='#1f77b4')
    
    ax2 = ax1.twinx()
    ax2.bar(df_target['trading_date'], df_target['daily_sentiment_score'], color='orange', alpha=0.4, label='Sentiment Score', width=1)
    ax2.set_ylabel('Sentiment (Negative/Positive)', color='orange', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='orange')
    
    plt.tight_layout()
    plt.savefig('1_price_vs_sentiment.png')
    print("-> 1_price_vs_sentiment.png saved")

    # 2. Price vs Insider Flow (Dual Axis)
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.set_title(f'{target_ticker}: Insider Trading Signals')
    ax1.plot(df_target['trading_date'], df_target['close_price'], color='black', alpha=0.6, label='Price')
    ax1.set_ylabel('Price ($)')
    
    ax2 = ax1.twinx()
    colors = ['#2ca02c' if x > 0 else '#d62728' for x in df_target['net_value_flow']]
    ax2.bar(df_target['trading_date'], df_target['net_value_flow'], color=colors, alpha=0.6, width=2)
    ax2.set_ylabel('Net Insider Flow ($)', fontsize=12)
    
    # Custom legend
    from matplotlib.lines import Line2D
    custom_lines = [Line2D([0], [0], color='#2ca02c', lw=4), Line2D([0], [0], color='#d62728', lw=4)]
    ax2.legend(custom_lines, ['Buy', 'Sell'], loc='upper left')
    
    plt.tight_layout()
    plt.savefig('2_price_vs_insider.png')
    print("-> 2_price_vs_insider.png saved")

    # 3. Correlation Matrix
    cols = ['close_price', 'volume_1d', 'daily_sentiment_score', 'net_value_flow', 'price_change_pct_1d']
    corr = df_target[cols].corr()
    
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, cmap='RdBu_r', center=0, fmt=".2f", linewidths=.5)
    plt.title(f'Factor Correlation Matrix - {target_ticker}')
    plt.tight_layout()
    plt.savefig('3_correlation_matrix.png')
    print("-> 3_correlation_matrix.png saved")

    # 4. Global: Insider Battle (Grouped Bar Chart)
    insider_agg = df_insider.groupby('ticket')[['total_shares_bought', 'total_shares_sold']].sum().reset_index()
    insider_agg['total_shares_sold_abs'] = insider_agg['total_shares_sold'].abs() # Absolute for visualization
    
    plt.figure(figsize=(10, 6))
    x = range(len(insider_agg))
    width = 0.35
    plt.bar(x, insider_agg['total_shares_bought'], width, label='Buy (Shares)', color='#2ca02c')
    plt.bar([i + width for i in x], insider_agg['total_shares_sold_abs'], width, label='Sell (Shares)', color='#d62728')
    
    plt.xticks([i + width/2 for i in x], insider_agg['ticket'])
    plt.title("Global View: Insider Buy vs Sell Volume")
    plt.ylabel("Number of Shares")
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    plt.savefig('4_insider_global_battle.png')
    print("-> 4_insider_global_battle.png saved")

    # 5. Volatility vs News Intensity (Boxplot)
    plt.figure(figsize=(10, 6))
    # We remove extreme outliers for better visualization if necessary, but here we keep them
    sns.boxplot(x='sentiment_intensity', y='price_change_pct_1d', data=merged_df, palette="Blues")
    plt.title("Impact of News Intensity on Price Volatility")
    plt.xlabel("Sentiment Intensity (0=Neutral, >0=Strong)")
    plt.ylabel("Daily Price Change (%)")
    plt.axhline(0, color='gray', linestyle='--', alpha=0.5)
    plt.savefig('5_volatility_sentiment.png')
    print("-> 5_volatility_sentiment.png saved")

    # 6. Scatter: News Volume vs Trading Volume
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='daily_news_count', y='volume_change_1d', data=merged_df, hue='ticket', s=100, alpha=0.8)
    plt.title("The 'Hype' Effect: News Qty vs Trading Volume Increase")
    plt.xlabel("Number of News Articles per Day")
    plt.ylabel("Share Volume Change")
    plt.grid(True, alpha=0.3)
    plt.axhline(0, color='black', linewidth=1)
    plt.savefig('6_news_volume_scatter.png')
    print("-> 6_news_volume_scatter.png saved")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    try:
        # 1. Connect
        engine = get_db_connection()
        
        # 2. Load
        df_t, df_n, df_i = load_data(engine)
        
        # 3. Process
        df_t, df_n, df_i, df_merged = process_data(df_t, df_n, df_i)
        
        # 4. Generate Visualizations
        generate_visuals(df_t, df_n, df_i, df_merged)
        
        print("\n--- Process completed successfully! Images saved in the folder. ---")
        
    except Exception as e:
        print(f"Fatal execution error: {e}")