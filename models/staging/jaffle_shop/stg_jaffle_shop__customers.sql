with 

source as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

transformed as (
  select 
    id as customer_id,
    last_name,
    first_name,
    first_name || ' ' || last_name as full_name
  from source
)

select * from transformed