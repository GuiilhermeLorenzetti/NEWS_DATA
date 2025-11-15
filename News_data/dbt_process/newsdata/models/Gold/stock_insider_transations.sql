WITH daily_insider_summary AS (
    SELECT 
        symbol,
        transaction_date,
        
        -- Contagem de insiders únicos que operaram no dia
        COUNT(DISTINCT name) AS distinct_insiders_active,
        
        -- Volume de Compra/Venda (ações)
        SUM(CASE WHEN "change" > 0 THEN "change" ELSE 0 END) AS total_shares_bought,
        SUM(CASE WHEN "change" < 0 THEN "change" ELSE 0 END) AS total_shares_sold, -- Valor será negativo
        SUM("change") AS net_shares_flow,
        
        -- Volume de Compra/Venda (financeiro)
        SUM(CASE WHEN "change" > 0 THEN transaction_price * "change" ELSE 0 END) AS total_value_bought,
        SUM(CASE WHEN "change" < 0 THEN transaction_price * "change" ELSE 0 END) AS total_value_sold, -- Valor será negativo
        SUM(transaction_price * "change") AS net_value_flow,
        
        -- Contagem de transações
        COUNT(*) AS total_transactions_count
        
    FROM 
        silver.insider_transactions_stocks -- Assumindo que sua tabela silver se chama assim
    GROUP BY 
        symbol, transaction_date
),

-- Passo 2: Aplicar funções de janela para enriquecimento (o "estilo" que você pediu)
final_metrics AS (
    SELECT
        -- Chaves e dados base
        dis.symbol, -- Chave: O ticker da ação (ex: AAPL, NFLX)
        dis.transaction_date, -- Chave: A data da(s) transação(ões)
        
        -- Métricas do Dia (da CTE)
        dis.distinct_insiders_active, -- Coluna: Número de insiders únicos ativos no dia
        dis.total_shares_bought, -- Coluna: Total de ações compradas por insiders no dia
        dis.total_shares_sold, -- Coluna: Total de ações vendidas por insiders no dia (negativo)
        dis.net_shares_flow, -- Coluna: Saldo líquido de ações (compradas - vendidas) no dia
        dis.net_value_flow, -- Coluna: Saldo líquido financeiro (valor comprado - valor vendido) no dia
        dis.total_transactions_count, -- Coluna: Número de transações de insiders no dia
        
        -- Métricas Comparativas (D-1 vs D-prev_active)
        LAG(dis.transaction_date, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date) AS previous_activity_date, -- Coluna: Data da última atividade de insider anterior
        -- Nota: DATE_DIFF é do BigQuery/Spark. Em PostgreSQL, use a subtração de datas.
        (dis.transaction_date - LAG(dis.transaction_date, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date)) AS days_since_last_activity, -- Coluna: Dias desde a última atividade de insider (PostgreSQL)
        
        LAG(dis.net_shares_flow, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date) AS previous_day_net_shares_flow, -- Coluna: Fluxo líquido de ações do dia de atividade anterior
        dis.net_shares_flow - LAG(dis.net_shares_flow, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date) AS change_in_net_shares_flow, -- Coluna: Variação no fluxo líquido de ações vs. dia anterior
        
        -- Métricas de Janela Móvel (Rolling) - (Baseado nos últimos 5 *dias de atividade de insider*)
        -- Este é o mesmo estilo 'ROWS BETWEEN 4 PRECEDING' do seu exemplo
        SUM(dis.net_shares_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_net_shares_flow, -- Coluna: Fluxo líquido de ações nos últimos 5 dias de atividade
        SUM(dis.net_value_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_net_value_flow, -- Coluna: Fluxo líquido financeiro nos últimos 5 dias de atividade
        AVG(dis.net_shares_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_avg_net_shares, -- Coluna: Média do fluxo líquido de ações nos últimos 5 dias de atividade
        SUM(dis.distinct_insiders_active) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_active_insiders, -- Coluna: Contagem de insiders ativos (não-únicos) nos últimos 5 dias de atividade
        SUM(dis.total_transactions_count) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_transaction_count, -- Coluna: Soma de transações nos últimos 5 dias de atividade
        
        -- Métricas Cumulativas (Desde o início)
        SUM(dis.net_shares_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_net_shares_flow, -- Coluna: Fluxo líquido acumulado de ações (posição histórica)
        SUM(dis.net_value_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_net_value_flow, -- Coluna: Fluxo líquido financeiro acumulado
        SUM(dis.total_transactions_count) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_transaction_count, -- Coluna: Contagem acumulada de transações

        -- Sinais e Indicadores (Exemplos)
        SIGN(SUM(dis.net_shares_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) AS rolling_5_day_sentiment, -- Coluna: Sentimento (1=compra líquida, -1=venda líquida, 0=neutro) nos últimos 5 dias de atividade
        CASE 
            WHEN (dis.transaction_date - LAG(dis.transaction_date, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date)) <= 3
            THEN 1 ELSE 0 
        END AS is_activity_cluster -- Coluna: Flag (1/0) se a atividade faz parte de um 'cluster' (<= 3 dias da atividade anterior) (PostgreSQL)

    FROM 
        daily_insider_summary dis
)
-- Seleção final para a tabela ou view 'gold'
SELECT 
    *
FROM 
    final_metrics;