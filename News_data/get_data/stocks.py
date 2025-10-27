import yfinance as yf
import pandas as pd
import psycopg2
import hashlib
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

def generate_hash(ticket: str, date: str) -> str:
    """
    Gera um hash único baseado no ticket e na data
    """
    hash_string = f"{ticket}_{date}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def get_db_connection():
    """
    Estabelece conexão com o banco PostgreSQL
    """
    try:
        connection = psycopg2.connect(
            host=os.getenv('HOSTNAME'),
            port=os.getenv('PORT'),
            database=os.getenv('DATABASE'),
            user=os.getenv('DATABASE_USER', 'postgres'),  # Assumindo usuário padrão se não especificado
            password=os.getenv('DATABASE_PASS')
        )
        return connection
    except Exception as e:
        print(f"Erro ao conectar com o banco: {e}")
        return None

def insert_stock_data(df: pd.DataFrame):
    """
    Insere dados no banco PostgreSQL com verificação de duplicatas
    """
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Prepara os dados para inserção
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
        
        # Query para inserir com ON CONFLICT (evita duplicatas)
        insert_query = """
        INSERT INTO bronze.stocks (hash, Ticket, Date, Close, High, Low, Open, Volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (hash) DO NOTHING
        """
        
        cursor.executemany(insert_query, insert_data)
        connection.commit()
        
        print(f"Dados inseridos no banco com sucesso! {len(insert_data)} registros processados.")
        return True
        
    except Exception as e:
        print(f"Erro ao inserir dados no banco: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def get_multiple_stocks(tickets: list, period: str, save_to_db: bool = True, filename: str = "raw_data/stock_data.csv"):
    all_data = []
    
    for ticket in tickets:
        print(f"Baixando dados de {ticket}...")
        data = yf.download(ticket, period=period, progress=False)
        
        # Remove MultiIndex se existir (achata as colunas)
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        # Reseta o index para transformar Date em coluna
        data = data.reset_index()
        
        # Adiciona a coluna Ticket
        data['Ticket'] = ticket
        
        all_data.append(data)
    
    # Combina todos os DataFrames
    df_combined = pd.concat(all_data, ignore_index=True)
    
    # Reorganiza as colunas: Ticket e Date primeiro
    cols = ['Ticket', 'Date'] + [col for col in df_combined.columns if col not in ['Ticket', 'Date']]
    df_combined = df_combined[cols]
    
    if save_to_db:
        # Salva no banco de dados
        success = insert_stock_data(df_combined)
        if success:
            print("Dados salvos no banco PostgreSQL!")
        else:
            print("Erro ao salvar no banco. Salvando como CSV...")
            df_combined.to_csv(filename, index=False)
            print(f"Dados salvos em: {filename}")
    else:
        # Salva como CSV (comportamento original)
        df_combined.to_csv(filename, index=False)
        print(f"Dados salvos em: {filename}")
    
    return df_combined


# Uso
tickets = ["AAPL", "META", "NVDA", "NFLX"]
df = get_multiple_stocks(tickets, period="10d", save_to_db=True)
