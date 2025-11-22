-- OBT News with sentiment counters
SELECT 
    sn.ticket,
    DATE(sn.published_at) as news_date,
    -- Daily counters
    COUNT(*) as daily_news_count,
    SUM(CASE WHEN sn.news_sentiment = 'good' THEN 1 ELSE 0 END) as daily_good_news_count,
    SUM(CASE WHEN sn.news_sentiment = 'bad' THEN 1 ELSE 0 END) as daily_bad_news_count,
    SUM(CASE WHEN sn.news_sentiment = 'neutral' THEN 1 ELSE 0 END) as daily_neutral_news_count,
    -- Daily percentages
    ROUND(SUM(CASE WHEN sn.news_sentiment = 'good' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as daily_good_news_pct,
    ROUND(SUM(CASE WHEN sn.news_sentiment = 'bad' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as daily_bad_news_pct,
    ROUND(SUM(CASE WHEN sn.news_sentiment = 'neutral' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as daily_neutral_news_pct,
    -- Rolling 5-day counters (including current day)
    SUM(COUNT(*)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as rolling_5d_news_count,
    SUM(SUM(CASE WHEN sn.news_sentiment = 'good' THEN 1 ELSE 0 END)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as rolling_5d_good_news_count,
    SUM(SUM(CASE WHEN sn.news_sentiment = 'bad' THEN 1 ELSE 0 END)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as rolling_5d_bad_news_count,
    SUM(SUM(CASE WHEN sn.news_sentiment = 'neutral' THEN 1 ELSE 0 END)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as rolling_5d_neutral_news_count,
    -- Sentiment Score (good: +1, bad: -1, neutral: 0)
    SUM(CASE 
        WHEN sn.news_sentiment = 'good' THEN 1 
        WHEN sn.news_sentiment = 'bad' THEN -1 
        ELSE 0 
    END) as daily_sentiment_score,
    SUM(SUM(CASE 
        WHEN sn.news_sentiment = 'good' THEN 1 
        WHEN sn.news_sentiment = 'bad' THEN -1 
        ELSE 0 
    END)) OVER (
        PARTITION BY sn.ticket 
        ORDER BY DATE(sn.published_at) 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as rolling_5d_sentiment_score
FROM {{ ref('news') }} sn
GROUP BY sn.ticket, DATE(sn.published_at)