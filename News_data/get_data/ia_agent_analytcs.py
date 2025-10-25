import pandas as pd
from groq import Groq
import os
from dotenv import load_dotenv


load_dotenv()

df = pd.read_csv('news_data.csv')

news = df['description']

api_groq = os.getenv('API_GROQ')

groq_cliente = Groq(api_key=api_groq)

def analyze_news(news_text):
    """Analisa uma única notícia e retorna 'good' ou 'bad'"""
    response = groq_cliente.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {
                "role": "user",
                "content": f'Is the following news headline good or bad? Headline: {news_text}. Only answer with "good" or "bad".'
            }
        ],
        temperature=0.0
    )
    return response.choices[0].message.content

# Processar cada linha do DataFrame
sentiment_results = []

print("Processando notícias...")
for index, row in df.iterrows():
    print(f"Processando linha {index + 1}/{len(df)}")
    sentiment = analyze_news(row['description'])
    sentiment_results.append(sentiment)

# Adicionar os resultados como nova coluna
df['sentiment'] = sentiment_results

# Salvar o DataFrame atualizado
df.to_csv('raw_data/teste.csv', index=False)

print("Processamento concluído! Arquivo salvo como 'news_data_with_sentiment.csv'")
print(f"Total de notícias processadas: {len(df)}")
print(f"Distribuição de sentimentos:")
print(df['sentiment'].value_counts())