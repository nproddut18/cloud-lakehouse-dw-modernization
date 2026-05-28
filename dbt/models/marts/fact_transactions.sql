-- fact_transactions: grain = one row per payment transaction
-- Incremental merge on txn_id

{{
  config(
    materialized  = 'incremental',
    unique_key    = 'txn_id',
    incremental_strategy = 'merge',
    dist          = 'customer_sk',
    sort          = ['txn_date_sk', 'customer_sk']
  )
}}

with txns as (
    select * from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        where _batch_date > (select max(_batch_date) from {{ this }})
    {% endif %}
),

orders as (
    select order_sk, order_id, customer_sk from {{ ref('fact_orders') }}
),

final as (
    select
        t.txn_id,
        o.order_sk,
        o.customer_sk,
        cast(to_char(t.txn_timestamp::date, 'YYYYMMDD') as int)  as txn_date_sk,
        t.txn_timestamp,
        t.amount,
        t.currency,
        t.payment_method,
        t.is_fraud,
        case
            when t.amount > 10000 then 'HIGH'
            when t.amount > 1000  then 'MEDIUM'
            else 'LOW'
        end                                                       as risk_tier,
        current_timestamp                                         as dbt_updated_at,
        t._batch_date

    from txns t
    left join orders o using (order_id)
)

select * from final
