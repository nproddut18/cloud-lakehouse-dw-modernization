-- stg_transactions: payment transactions joined to order context

with source as (
    select * from {{ source('bronze', 'transactions') }}
),

cleaned as (
    select
        txn_id,
        order_id,
        cast(txn_timestamp as timestamp)                as txn_timestamp,
        round(cast(amount as decimal(18,2)), 2)         as amount,
        upper(trim(currency))                           as currency,
        lower(trim(payment_method))                     as payment_method,
        coalesce(is_fraud, false)                       as is_fraud,
        _ingested_at,
        _batch_date

    from source
    where txn_id  is not null
      and order_id is not null
      and amount   > 0
)

select * from cleaned
