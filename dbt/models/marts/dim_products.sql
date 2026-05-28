-- dim_products: static product dimension (Type 1 — overwrite on change)

{{
  config(
    materialized = 'table',
    dist = 'product_sk',
    sort = ['category', 'product_id']
  )
}}

with source as (
    select * from {{ source('bronze', 'products') }}
),

final as (
    select
        {{ dbt_utils.surrogate_key(['product_id']) }}   as product_sk,
        product_id,
        trim(product_name)                              as product_name,
        trim(category)                                  as category,
        trim(sub_category)                              as sub_category,
        round(cast(unit_cost as decimal(18,2)), 2)      as unit_cost,
        round(cast(list_price as decimal(18,2)), 2)     as list_price,
        coalesce(is_active, true)                       as is_active,
        current_timestamp                               as dbt_updated_at

    from source
    where product_id is not null
)

select * from final
