-- stg_customers: clean, renamed, type-cast customer records from the raw S3/Glue layer

with source as (
    select * from {{ source('bronze', 'customers') }}
),

renamed as (
    select
        customer_id                                     as customer_id,
        trim(lower(first_name))                         as first_name,
        trim(lower(last_name))                          as last_name,
        trim(lower(email))                              as email,
        upper(country)                                  as country,
        cast(created_at as timestamp)                   as created_at,
        coalesce(is_active, false)                      as is_active,
        _ingested_at,
        _batch_date

    from source
    where customer_id is not null
      and email is not null
)

select * from renamed
