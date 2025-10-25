import yfinance as yf
import pandas as pd


def get_multiple_stocks(tickets: list, period: str, filename: str = "raw_data/stock_data.csv"):
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
    
    # Salva
    df_combined.to_csv(filename, index=False)
    print(f"\nDados salvos em: {filename}")
    
    return df_combined


# Uso
tickets = ["AAPL", "META", "NVDA", "NFLX"]
df = get_multiple_stocks(tickets, period="10d")
