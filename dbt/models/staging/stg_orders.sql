-- stg_orders: cleansed and validated order records

with source as (
    select * from {{ source('bronze', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        product_id,
        cast(order_date as date)                        as order_date,
        round(cast(order_amount as decimal(18,2)), 2)   as order_amount,
        cast(quantity as integer)                       as quantity,
        lower(trim(status))                             as status,
        _ingested_at,
        _batch_date

    from source
    where order_id     is not null
      and customer_id  is not null
      and order_amount > 0
      and order_date  >= '{{ var("start_date") }}'
)

select * from renamed
