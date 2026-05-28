-- dim_customers: SCD Type 2 customer dimension
-- Uses the scd_type2 macro to track historical attribute changes

{{
  config(
    materialized = 'table',
    dist = 'customer_sk',
    sort = ['effective_start_date', 'customer_id']
  )
}}

{{ scd_type2(
    source_ref     = ref('int_customer_orders'),
    unique_key     = 'customer_id',
    surrogate_key  = 'customer_sk',
    tracked_cols   = ['email', 'country', 'is_active', 'lifetime_value'],
    effective_from = 'effective_start_date',
    effective_to   = 'effective_end_date',
    current_flag   = 'is_current'
) }}
