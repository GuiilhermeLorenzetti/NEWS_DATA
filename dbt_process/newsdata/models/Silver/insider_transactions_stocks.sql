with insider_transactions_source as (
    select
        *
    from {{  source('bronze', 'insider_transactions') }} 
),
insider_transactions_source_type as (
    select
        cast(hash as varchar) as insider_transactions_hash,
        upper(cast(symbol as varchar)) as symbol,
        cast(name as varchar) as name,
        cast(share as bigint) as share,
        cast(change as bigint) as change,
        cast(filing_date as date) as filing_date,
        cast(transaction_date as date) as transaction_date,
        cast(transaction_price as decimal(18, 4)) as transaction_price,
        cast(transaction_code as varchar) as transaction_code,
        cast(current_timestamp as timestamp) as updated_at
    from insider_transactions_source
)
select * from insider_transactions_source_type