# Primeiro, instale a biblioteca: pip install yfinance
import yfinance as yf

# Baixar os dados dos últimos 2 dias para a Apple
data = yf.download("AAPL", period="2d")

if not data.empty and len(data) >= 2:
    # Pegar os valores de fechamento
    preco_fechamento_hoje = data['Close'].iloc[-1]
    preco_fechamento_ontem = data['Close'].iloc[-2]

    # Calcular a variação
    variacao = preco_fechamento_hoje - preco_fechamento_ontem
    variacao_percentual = (variacao / preco_fechamento_ontem) * 100

    print(f"Ticker: AAPL")
    print(f"Preço de Fechamento: ${preco_fechamento_hoje:.2f}")
    print(f"Variação em relação ao dia anterior: ${variacao:.2f} ({variacao_percentual:.2f}%)")
else:
    print("Não foi possível obter os dados para calcular a variação.")

# O resultado seria algo como:
# Ticker: AAPL
# Preço de Fechamento: $172.20
# Variação em relação ao dia anterior: $2.30 (1.35%)