with stocks_source as (
    select
        *
    from {{  source('bronze', 'stocks') }} 
),
stocks_type as (
    select
        cast(hash as varchar) as stock_hash,
        upper(cast(ticket as varchar)) as ticket,
        cast(date as date) as trading_date,
        cast(close as decimal(18, 4)) as close_price,
        cast(high as decimal(18, 4)) as high_price,
        cast(low as decimal(18, 4)) as low_price,
        cast(open as decimal(18, 4)) as open_price,
        cast(volume as bigint) as trade_volume,
        cast(current_timestamp as timestamp) as updated_at
    from stocks_source
)
select * from stocks_type