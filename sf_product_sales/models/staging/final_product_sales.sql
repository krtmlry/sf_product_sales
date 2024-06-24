{{config(materialized='view')}}

with final_product_sales as (
	select
		row_number() over(order by order_date asc) as order_sk_id,
		order_id,
		product,
		price_each,
		qty_ordered,
		order_date,
		purchase_address,
		store,
		payment_method
	from {{ ref('t_cast_types') }}
	where extract(year from order_date) = 2019
	order by order_date asc
)

select *
from final_product_sales