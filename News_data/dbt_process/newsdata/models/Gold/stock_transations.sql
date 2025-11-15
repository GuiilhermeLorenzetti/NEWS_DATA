SELECT 
    ss.ticket,
    ss.trading_date,
    -- Preços básicos
    ss.close_price,
    ss.low_price,
    ss.trade_volume,
    -- Variação diária (D-1)
    LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) as close_price_yesterday,
    ss.close_price - LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) as variacao_preco_d1,
    ROUND(((ss.close_price - LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date)) / 
           NULLIF(LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date), 0) * 100), 2) as variacao_percentual_d1,
    -- Variação volume D-1
    LAG(ss.trade_volume, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) as volume_d1,
    ss.trade_volume - LAG(ss.trade_volume, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) as variacao_volume_d1,
    -- Médias móveis (5 dias)
    ROUND(AVG(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) as mm5_close_price,
    ROUND(AVG(ss.trade_volume) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 0) as mm5_volume,
    -- Máximos e mínimos dos últimos 5 dias
    MAX(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as max_5d_close,
    MIN(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as min_5d_close,
    MIN(ss.low_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as min_5d_low,
    MAX(ss.trade_volume) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as max_5d_volume,
    -- Volatilidade (range dos últimos 5 dias)
    ROUND((MAX(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - 
           MIN(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)), 2) as range_5d,
    -- Posição relativa ao range de 5 dias (0 = mínimo, 100 = máximo)
    ROUND(((ss.close_price - MIN(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) /
           NULLIF((MAX(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - 
                   MIN(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)), 0) * 100), 2) as percentil_5d,
    -- Desvio padrão dos últimos 5 dias (volatilidade)
    ROUND(STDDEV(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) as stddev_5d_close, 
    -- Distância da média móvel
    ROUND(ss.close_price - AVG(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) as distancia_mm5,
    ROUND(((ss.close_price - AVG(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) /
           NULLIF(AVG(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 0) * 100), 2) as distancia_percentual_mm5,
    -- Tendência (contagem de dias consecutivos de alta/baixa)
    CASE 
        WHEN ss.close_price > LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) THEN 1
        WHEN ss.close_price < LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) THEN -1
        ELSE 0
    END as direcao_d1
FROM silver.stocks ss;