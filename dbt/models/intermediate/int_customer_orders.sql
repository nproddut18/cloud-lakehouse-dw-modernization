-- int_customer_orders: aggregate order metrics per customer for use in dim/fact models

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

agg as (
    select
        o.customer_id,
        count(distinct o.order_id)              as total_orders,
        sum(o.order_amount)                     as lifetime_value,
        avg(o.order_amount)                     as avg_order_value,
        min(o.order_date)                       as first_order_date,
        max(o.order_date)                       as last_order_date,
        count(distinct case when o.status = 'returned' then o.order_id end)
                                                as total_returns

    from orders o
    group by 1
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.is_active,
    c.created_at,
    a.total_orders,
    a.lifetime_value,
    a.avg_order_value,
    a.first_order_date,
    a.last_order_date,
    a.total_returns

from customers c
left join agg a using (customer_id)
