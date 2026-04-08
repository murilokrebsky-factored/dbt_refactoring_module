with 
-- import ctes

customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

orders as (
    select * from {{ source('jaffle_shop', 'orders') }}
),

payments as (
    select * from {{ source('stripe', 'payment') }}
),

-- Logical ctes

p as (
    select
        orderid                 as order_id,
        max(created)            as payment_finalized_date,
        sum(amount) / 100.0     as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select
        orders.id                   as order_id,
        orders.user_id              as customer_id,
        orders.order_date           as order_placed_at,
        orders.status               as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name                as customer_first_name,
        c.last_name                 as customer_last_name
    from orders
    left join p
        on orders.id = p.order_id
    left join customers c
        on orders.user_id = c.id
),

lifetime_value as (
    select
    order_id,
    sum(total_amount_paid) over(
        partition by customer_id 
        order by order_id
        rows between unbounded preceding and current row
    ) as cumulative_lifetime_value
    from paid_orders
    order by order_id
),

-- final cte

final as (select
    p.*,
    row_number() over (order by p.order_id)                             as transaction_seq,
    row_number() over (partition by p.customer_id order by p.order_id)    as customer_sales_seq,

    -- Defines whether or not the customer is a new customer
    case
        when (
            rank() over(
                partition by customer_id
                order by p.order_placed_at, p.order_id
            ) = 1
        ) then 'new'
        else 'return'
    end as nvsr,

    lifetime_value.cumulative_lifetime_value,
    min(p.order_placed_at) over(partition by p.customer_id) as fdos
    from paid_orders p
    left outer join lifetime_value
        on lifetime_value.order_id = p.order_id
    order by order_id
)
-- final select statement

select * from final