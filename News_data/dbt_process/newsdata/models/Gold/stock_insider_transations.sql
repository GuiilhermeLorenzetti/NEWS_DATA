WITH daily_insider_summary AS (
    SELECT 
        symbol,
        transaction_date,
        
        -- Count of unique insiders active on the day
        COUNT(DISTINCT name) AS distinct_insiders_active,
        
        -- Buy/Sell Volume (shares)
        SUM(CASE WHEN "change" > 0 THEN "change" ELSE 0 END) AS total_shares_bought,
        SUM(CASE WHEN "change" < 0 THEN "change" ELSE 0 END) AS total_shares_sold, -- Value will be negative
        SUM("change") AS net_shares_flow,
        
        -- Buy/Sell Volume (financial)
        SUM(CASE WHEN "change" > 0 THEN transaction_price * "change" ELSE 0 END) AS total_value_bought,
        SUM(CASE WHEN "change" < 0 THEN transaction_price * "change" ELSE 0 END) AS total_value_sold, -- Value will be negative
        SUM(transaction_price * "change") AS net_value_flow,
        
        -- Transaction count
        COUNT(*) AS total_transactions_count
        
    FROM 
        {{ ref('insider_transactions_stocks') }}
    GROUP BY 
        symbol, transaction_date
),

-- Step 2: Apply window functions for enrichment
final_metrics AS (
    SELECT
        -- Keys and base data
        dis.symbol, -- Key: Stock ticker (e.g., AAPL, NFLX)
        dis.transaction_date, -- Key: Transaction date
        
        -- Daily Metrics (from CTE)
        dis.distinct_insiders_active, -- Column: Number of unique active insiders on the day
        dis.total_shares_bought, -- Column: Total shares bought by insiders on the day
        dis.total_shares_sold, -- Column: Total shares sold by insiders on the day (negative)
        dis.net_shares_flow, -- Column: Net shares flow (bought - sold) on the day
        dis.net_value_flow, -- Column: Net financial flow (value bought - value sold) on the day
        dis.total_transactions_count, -- Column: Number of insider transactions on the day
        
        -- Comparative Metrics (D-1 vs D-prev_active)
        LAG(dis.transaction_date, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date) AS previous_activity_date, -- Column: Date of the last previous insider activity
        -- Note: DATE_DIFF is for BigQuery/Spark. In PostgreSQL, use date subtraction.
        (dis.transaction_date - LAG(dis.transaction_date, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date)) AS days_since_last_activity, -- Column: Days since last insider activity
        
        LAG(dis.net_shares_flow, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date) AS previous_day_net_shares_flow, -- Column: Net shares flow of the previous activity day
        dis.net_shares_flow - LAG(dis.net_shares_flow, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date) AS change_in_net_shares_flow, -- Column: Change in net shares flow vs. previous day
        
        -- Rolling Window Metrics (Based on last 5 *insider activity days*)
        -- This is the same style 'ROWS BETWEEN 4 PRECEDING' as your example
        SUM(dis.net_shares_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_net_shares_flow, -- Column: Net shares flow in the last 5 activity days
        SUM(dis.net_value_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_net_value_flow, -- Column: Net financial flow in the last 5 activity days
        AVG(dis.net_shares_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_avg_net_shares, -- Column: Average net shares flow in the last 5 activity days
        SUM(dis.distinct_insiders_active) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_active_insiders, -- Column: Count of active insiders (non-unique) in the last 5 activity days
        SUM(dis.total_transactions_count) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_transaction_count, -- Column: Sum of transactions in the last 5 activity days
        
        -- Cumulative Metrics (Since inception)
        SUM(dis.net_shares_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_net_shares_flow, -- Column: Cumulative net shares flow (historical position)
        SUM(dis.net_value_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_net_value_flow, -- Column: Cumulative net financial flow
        SUM(dis.total_transactions_count) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_transaction_count,

        -- Signals and Indicators (Examples)
        SIGN(SUM(dis.net_shares_flow) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) AS rolling_5_day_sentiment, -- Column: Sentiment (1=net buy, -1=net sell, 0=neutral) in the last 5 activity days
        CASE 
            WHEN (dis.transaction_date - LAG(dis.transaction_date, 1) OVER (PARTITION BY dis.symbol ORDER BY dis.transaction_date)) <= 3
            THEN 1 ELSE 0 
        END AS is_activity_cluster -- Column: Flag (1/0) if activity is part of a 'cluster' (<= 3 days from previous activity)
    FROM 
        daily_insider_summary dis
)
-- Final selection for the gold table or view
SELECT 
    *
FROM 
    final_metrics