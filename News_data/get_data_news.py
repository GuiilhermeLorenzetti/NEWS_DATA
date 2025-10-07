import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

# Carrega as variáveis do arquivo .env
load_dotenv()

API_KEY = os.getenv('API_KEY_NEWS')

# Lista de empresas para buscar
companies = ["Apple", "Meta", "Nvidia", "Netflix"]

url = 'https://newsapi.org/v2/everything'

def get_data(url, query, api_key):
    """Busca notícias para uma empresa específica"""
    
    # Parâmetros otimizados para a NewsAPI
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
                
                # Criar DataFrame com os artigos
                if articles:
                    df = pd.DataFrame(articles)
                    df['company'] = query
                    return df[['url','company', 'publishedAt', 'title', 'description']]
                else:
                    print("Nenhum artigo encontrado para esta empresa")
                    return pd.DataFrame()
                    
            else:
                print("Erro na resposta da API:")
                print(data)
                return pd.DataFrame()
                
        elif response.status_code == 401:
            print("❌ Erro de autenticação - Verifique sua API Key")
            return pd.DataFrame()
        elif response.status_code == 429:
            print("❌ Limite de requisições excedido - Aguarde um momento")
            return pd.DataFrame()
        else:
            print(f"❌ Erro HTTP {response.status_code}")
            print(response.text)
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de conexão: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return pd.DataFrame()


def main():
    """Função principal que processa todas as empresas"""
    
    all_data = []
    
    for i, company in enumerate(companies):
        # Busca dados da empresa
        df = get_data(url, company, API_KEY)
        
        if not df.empty:
            all_data.append(df)
            print(f"✓ Dados de {company} adicionados ({len(df)} artigos)")
        
        # Delay entre requisições para evitar rate limit (exceto na última)
        if i < len(companies) - 1:
            print("Aguardando 2 segundos antes da próxima requisição...")
            time.sleep(2)
    
    # Concatena todos os DataFrames
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Salva em CSV
        final_df.to_csv('news_data_07_10.csv', index=False)
        
        print(f"\n{'='*60}")
        print(f"✓ Processo concluído!")
        print(f"Total de artigos salvos: {len(final_df)}")
        print(f"Dados salvos em 'news_data.csv'")
        print(f"\nResumo por empresa:")
        print(final_df['company'].value_counts())
    else:
        print("\n❌ Nenhum dado foi coletado")




if __name__ == "__main__":
    main()