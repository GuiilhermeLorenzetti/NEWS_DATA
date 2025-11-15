-- OBT Notícias com contadores de sentimento
SELECT 
    sn.ticket,
    DATE(sn.published_at) as news_date,
    -- Contadores do dia
    COUNT(*) as total_noticias_dia,
    SUM(CASE WHEN sn.news_sentiment = 'good' THEN 1 ELSE 0 END) as noticias_good_dia,
    SUM(CASE WHEN sn.news_sentiment = 'bad' THEN 1 ELSE 0 END) as noticias_bad_dia,
    SUM(CASE WHEN sn.news_sentiment = 'neutral' THEN 1 ELSE 0 END) as noticias_neutral_dia,
    -- Percentuais do dia
    ROUND(SUM(CASE WHEN sn.news_sentiment = 'good' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as perc_good_dia,
    ROUND(SUM(CASE WHEN sn.news_sentiment = 'bad' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as perc_bad_dia,
    ROUND(SUM(CASE WHEN sn.news_sentiment = 'neutral' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as perc_neutral_dia,
    -- Contadores últimos 5 dias (incluindo o dia atual)
    SUM(COUNT(*)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as total_noticias_5d,
    SUM(SUM(CASE WHEN sn.news_sentiment = 'good' THEN 1 ELSE 0 END)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as noticias_good_5d,
    SUM(SUM(CASE WHEN sn.news_sentiment = 'bad' THEN 1 ELSE 0 END)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as noticias_bad_5d,
    SUM(SUM(CASE WHEN sn.news_sentiment = 'neutral' THEN 1 ELSE 0 END)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as noticias_neutral_5d,
    -- Score de sentimento (good: +1, bad: -1, neutral: 0)
    SUM(CASE 
        WHEN sn.news_sentiment = 'good' THEN 1 
        WHEN sn.news_sentiment = 'bad' THEN -1 
        ELSE 0 
    END) as sentiment_score_dia,
    SUM(SUM(CASE 
        WHEN sn.news_sentiment = 'good' THEN 1 
        WHEN sn.news_sentiment = 'bad' THEN -1 
        ELSE 0 
    END)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as sentiment_score_5d
FROM silver.news sn
GROUP BY sn.ticket, DATE(sn.published_at);