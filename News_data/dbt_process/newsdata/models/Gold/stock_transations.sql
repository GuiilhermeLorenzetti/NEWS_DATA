SELECT 
    ss.ticket,
    ss.trading_date,
    -- Basic prices
    ss.close_price,
    ss.low_price,
    ss.trade_volume,
    -- Daily variation (D-1)
    LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) as close_price_yesterday,
    ss.close_price - LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) as price_change_1d,
    ROUND(((ss.close_price - LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date)) / 
           NULLIF(LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date), 0) * 100), 2) as price_change_pct_1d,
    -- Volume variation D-1
    LAG(ss.trade_volume, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) as volume_1d,
    ss.trade_volume - LAG(ss.trade_volume, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) as volume_change_1d,
    -- Moving averages (5 days)
    ROUND(AVG(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) as ma5_close_price,
    ROUND(AVG(ss.trade_volume) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 0) as ma5_volume,
    -- Max and min of last 5 days
    MAX(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as max_5d_close_price,
    MIN(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as min_5d_close_price,
    MIN(ss.low_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as min_5d_low_price,
    MAX(ss.trade_volume) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as max_5d_volume,
    -- Volatility (range of last 5 days)
    ROUND((MAX(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - 
           MIN(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)), 2) as price_range_5d,
    -- Position relative to 5-day range (0 = min, 100 = max)
    ROUND(((ss.close_price - MIN(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) /
           NULLIF((MAX(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - 
                   MIN(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)), 0) * 100), 2) as price_percentile_5d,
    -- Standard deviation of last 5 days (volatility)
    ROUND(STDDEV(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) as stddev_5d_close_price, 
    -- Distance from moving average
    ROUND(ss.close_price - AVG(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) as distance_from_ma5,
    ROUND(((ss.close_price - AVG(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) /
           NULLIF(AVG(ss.close_price) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 0) * 100), 2) as distance_pct_from_ma5,
    -- Trend (consecutive days of up/down)
    CASE 
        WHEN ss.close_price > LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) THEN 1
        WHEN ss.close_price < LAG(ss.close_price, 1) OVER (PARTITION BY ss.ticket ORDER BY ss.trading_date) THEN -1
        ELSE 0
    END as price_direction_1d
FROM {{ ref('stocks') }} ss