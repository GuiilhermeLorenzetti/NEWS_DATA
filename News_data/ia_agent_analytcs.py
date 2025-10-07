import pandas as pd
from groq import Groq
import os
from dotenv import load_dotenv

load_dotenv()

df = pd.read_csv('news_data.csv')

news = df['description']

api_groq = os.getenv('API_GROQ')

groq_cliente = Groq(api_key=api_groq)

def analyze_news(news):
    for new in news:
        response = groq_cliente.chat.completions.create(
            model="openai/gpt-oss-20b",
            messages = [
                {
                    "role": "user",
                    "content": f'Is the following news headline good or bad? Headline: {new}. Only answer with "good" or "bad".'
                }
            ],
            temperature=0.0
        )
        return response.choices[0].message.content

print(analyze_news(news))