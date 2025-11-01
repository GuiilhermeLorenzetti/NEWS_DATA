import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import psycopg2
import hashlib
from groq import Groq

# Carrega as variáveis do arquivo .env (que está na mesma pasta)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Configurações da API
API_KEY_NEWS = os.getenv('API_KEY_NEWS')
API_GROQ = os.getenv('API_GROQ')

# Lista de empresas para buscar
companies = ["Apple", "Meta", "Nvidia", "Netflix"]

# Configuração do cliente Groq
groq_client = Groq(api_key=API_GROQ)

def generate_hash(company: str, title: str, published_at: str) -> str:
    """
    Gera um hash único baseado na empresa, título e data de publicação
    """
    hash_string = f"{company}_{title}_{published_at}"
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
            user=os.getenv('DATABASE_USER', 'postgres'),
            password=os.getenv('DATABASE_PASS'),
            sslmode='require'
        )
        return connection
    except Exception as e:
        print(f"Erro ao conectar com o banco: {e}")
        return None

def analyze_news_sentiment(news_text):
    """
    Analisa uma única notícia e retorna 'good' ou 'bad'
    """
    try:
        response = groq_client.chat.completions.create(
            model="openai/gpt-oss-20b",
            messages=[
                {
                    "role": "user",
                    "content": f'Is the following news headline good or bad? Headline: {news_text}. Only answer with "good" or "bad".'
                }
            ],
            temperature=0.0
        )
        return response.choices[0].message.content.strip().lower()
    except Exception as e:
        print(f"Erro ao analisar sentimento: {e}")
        return "neutral"

def get_news_data(url, query, api_key):
    """
    Busca notícias para uma empresa específica
    """
    params = {
        'q': query,
        'apiKey': api_key,
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': 30,
        'domains': 'bloomberg.com,reuters.com,cnbc.com,techcrunch.com',
        'from': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    }
    
    try:
        response = requests.get(url, params=params)
        
        print(f"\n{'='*60}")
        print(f"Buscando notícias para: {query}")
        print(f"Status da resposta: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('status') == 'ok' and 'articles' in data:
                articles = data['articles']
                print(f"Total de artigos encontrados: {len(articles)}")
                
                if articles:
                    df = pd.DataFrame(articles)
                    df['company'] = query
                    return df[['url', 'company', 'publishedAt', 'title', 'description']]
                else:
                    print("Nenhum artigo encontrado para esta empresa")
                    return pd.DataFrame()
                    
            else:
                print("Erro na resposta da API:")
                print(data)
                return pd.DataFrame()
                
        elif response.status_code == 401:
            print("ERRO de autenticacao - Verifique sua API Key")
            return pd.DataFrame()
        elif response.status_code == 429:
            print("ERRO - Limite de requisicoes excedido - Aguarde um momento")
            return pd.DataFrame()
        else:
            print(f"ERRO HTTP {response.status_code}")
            print(response.text)
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"ERRO de conexao: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERRO inesperado: {e}")
        return pd.DataFrame()

def insert_news_data(df: pd.DataFrame):
    """
    Insere dados de notícias no banco PostgreSQL com verificação de duplicatas
    """
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Prepara os dados para inserção
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
        
        # Query para inserir com ON CONFLICT (evita duplicatas)
        insert_query = """
        INSERT INTO bronze.news (hash, company, title, description, url, published_at, sentiment)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (hash) DO NOTHING
        """
        
        cursor.executemany(insert_query, insert_data)
        connection.commit()
        
        print(f"Dados de notícias inseridos no banco com sucesso! {len(insert_data)} registros processados.")
        return True
        
    except Exception as e:
        print(f"Erro ao inserir dados no banco: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def create_news_table():
    """
    Cria a tabela de notícias no banco PostgreSQL se ela não existir
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
        print("Tabela bronze.news criada/verificada com sucesso!")
        return True
        
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def test_db_connection():
    """
    Testa a conexão com o banco de dados antes de iniciar o processamento
    """
    print("\n" + "="*60)
    print("TESTANDO CONEXAO COM O BANCO DE DADOS...")
    print("="*60)
    
    connection = get_db_connection()
    if connection is None:
        print("ERRO - FALHA: Nao foi possivel conectar ao banco de dados")
        print("Verifique suas credenciais no arquivo .env")
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"OK - Conexao estabelecida com sucesso!")
        print(f"  PostgreSQL version: {version[0]}")
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"ERRO ao testar conexao: {e}")
        if connection:
            connection.close()
        return False

def main():
    """
    Função principal que processa todas as empresas: coleta notícias, analisa sentimento e salva no banco
    """
    # Testa conexão com o banco ANTES de iniciar o processamento
    if not test_db_connection():
        print("\nINTERROMPENDO: Script nao sera executado devido a falha na conexao com o banco")
        return
    
    url = 'https://newsapi.org/v2/everything'
    
    # Cria a tabela se não existir
    print("\nVerificando/criando tabela no banco...")
    create_news_table()
    
    all_data = []
    
    for i, company in enumerate(companies):
        # Busca dados da empresa
        df = get_news_data(url, company, API_KEY_NEWS)
        
        if not df.empty:
            print(f"OK - Dados de {company} coletados ({len(df)} artigos)")
            
            # Analisa sentimento para cada notícia
            print(f"Analisando sentimento para {company}...")
            sentiments = []
            
            for index, row in df.iterrows():
                print(f"  Processando noticia {index + 1}/{len(df)}: {row['title'][:50]}...")
                sentiment = analyze_news_sentiment(row['description'])
                sentiments.append(sentiment)
                
                # Pequeno delay para evitar rate limit da API Groq
                time.sleep(0.5)
            
            # Adiciona coluna de sentimento
            df['sentiment'] = sentiments
            
            all_data.append(df)
            print(f"OK - Analise de sentimento para {company} concluida")
        
        # Delay entre requisições para evitar rate limit (exceto na última)
        if i < len(companies) - 1:
            print("Aguardando 2 segundos antes da próxima requisição...")
            time.sleep(2)
    
    # Concatena todos os DataFrames
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Salva no banco de dados
        print("\nSalvando dados no banco PostgreSQL...")
        success = insert_news_data(final_df)
        
        if success:
            print(f"\n{'='*60}")
            print(f"OK - Processo concluido!")
            print(f"Total de artigos processados: {len(final_df)}")
            print(f"Dados salvos no banco PostgreSQL!")
            print(f"\nResumo por empresa:")
            print(final_df['company'].value_counts())
            print(f"\nDistribuicao de sentimentos:")
            print(final_df['sentiment'].value_counts())
        else:
            print("Erro ao salvar no banco. Salvando como CSV de backup...")
            final_df.to_csv('raw_data/news_data_with_sentiment_backup.csv', index=False)
            print(f"Dados salvos em: news_data_with_sentiment_backup.csv")
    else:
        print("\nERRO - Nenhum dado foi coletado")

if __name__ == "__main__":
    main()
