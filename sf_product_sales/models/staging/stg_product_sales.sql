{{config(materialized='view')}}

with stg_product_sales as (
	select
		order_id,
		product,
		price_each,
		qty_ordered,
		(price_each * qty_ordered)::numeric(10,2) as total_price,
		order_date,
		purchase_address,
		store,
		payment_method
	from {{ ref('t_cast_types') }}
	where extract(year from order_date) = 2019
	order by order_date asc
)

select *
from stg_product_sales