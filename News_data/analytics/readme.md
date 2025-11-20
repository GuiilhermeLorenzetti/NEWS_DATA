📊 Market Intelligence: Análise Multivariada de Ações (Sentimento & Insiders)
🔎 Contexto do Projeto
Neste projeto, desenvolvi um pipeline de análise de dados financeiros para investigar a correlação entre três pilares fundamentais do mercado de ações: Preço (Price Action), Fluxo de Informação (News Sentiment) e Movimentação Interna (Insider Trading).

O objetivo não foi apenas plotar gráficos de preço, mas entender os "drivers" invisíveis que antecipam movimentos de mercado. Os dados foram processados em uma arquitetura Medallion (Bronze/Silver/Gold) e consumidos via Python diretamente de um Data Warehouse PostgreSQL.

🧠 Principais Insights da Análise
Ao cruzar os dados transacionais com o sentimento de notícias e operações de diretores (C-Level), três padrões claros emergiram:

1. A Batalha dos Insiders: Sinal de Alerta ou Realização de Lucros?
Uma análise superficial sugeriria que os insiders apenas venderam ações no período. No entanto, ao aprofundar no Net Value Flow (Fluxo Financeiro Líquido), identificamos um comportamento nuançado:

Volume de Venda Massivo: Existe uma pressão vendedora predominante, especialmente em papéis de alta performance como NVDA e META. Em NVDA, o volume de venda superou o de compra em quase 10x.

O "Smart Money" na Compra: Ao contrário do senso comum, houve compras estratégicas (barras verdes nos gráficos). Embora menores em volume financeiro, essas operações são estatisticamente mais relevantes pois ocorrem contra a tendência de liquidez interna. Quando um insider compra enquanto seus pares vendem, isso gera um forte sinal de confiança no longo prazo (valuation descontado).

2. A "Economia da Atenção" e Liquidez
Identifiquei uma correlação positiva direta (0.56) entre a contagem de notícias diárias e o volume negociado, independentemente do viés da notícia.

Insight: O mercado reage à presença da informação, não apenas à qualidade dela. Dias com pico de notícias ("Hype") atraem liquidez imediata, validando a tese de que algoritmos de HFT e Day Traders utilizam o fluxo de mídia como trigger de volatilidade, criando oportunidades de entrada/saída independente se a notícia é "Boa" ou "Ruim".

3. Intensidade do Sentimento como Vetor de Volatilidade
Utilizando boxplots para medir a dispersão de preço baseada no sentiment_score, ficou provado que notícias extremas (muito positivas ou muito negativas) alargam o range de preço do dia.

Diferente de dias com "sentimento neutro" (onde o preço tende a andar de lado), dias com alta intensidade de sentimento apresentam as maiores variações percentuais (price_change_pct). Isso sugere que estratégias de Long/Short ou opções (Volatility Arbitrage) são mais eficientes quando filtradas pela intensidade do fluxo de notícias.

🛠️ Stack Tecnológico Utilizado
Linguagem: Python 3.12

Banco de Dados: PostgreSQL (Render) via SQLAlchemy

Análise de Dados: Pandas & NumPy para manipulação vetorial.

Visualização: Matplotlib & Seaborn (focados em gráficos de eixo duplo para correlação).

Engenharia: Uso de variáveis de ambiente (.env) para segurança de credenciais e conexão direta com tabelas Gold.

📂 Visualizações Chave
(Aqui você insere as imagens que geramos, ex: 2_preco_vs_insider.png e 6_news_volume_scatter.png)

💡 Conclusão
Esta análise demonstra que operar ou analisar o preço isoladamente é ineficiente. A integração de dados alternativos (Notícias e Insiders) oferece uma vantagem competitiva ("Alpha"), permitindo antecipar picos de volatilidade e entender se uma queda de preço é um movimento de pânico do varejo ou uma saída estruturada da diretoria.