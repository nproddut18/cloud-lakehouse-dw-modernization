-- fact_orders: grain = one row per order line
-- Incremental model: on refresh, only new/changed orders are processed

{{
  config(
    materialized  = 'incremental',
    unique_key    = 'order_id',
    incremental_strategy = 'merge',
    dist          = 'customer_sk',
    sort          = ['order_date_sk', 'customer_sk'],
    on_schema_change = 'sync_all_columns'
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        where _batch_date > (select max(_batch_date) from {{ this }})
    {% endif %}
),

customers as (
    select customer_sk, customer_id from {{ ref('dim_customers') }}
    where is_current = true
),

products as (
    select product_sk, product_id from {{ ref('dim_products') }}
),

final as (
    select
        {{ dbt_utils.surrogate_key(['o.order_id']) }}   as order_sk,
        o.order_id,
        c.customer_sk,
        p.product_sk,
        cast(to_char(o.order_date, 'YYYYMMDD') as int)  as order_date_sk,
        o.order_date,
        o.order_amount,
        o.quantity,
        o.status,
        current_timestamp                               as dbt_updated_at,
        o._batch_date

    from orders o
    left join customers c using (customer_id)
    left join products  p using (product_id)
)

select * from final
