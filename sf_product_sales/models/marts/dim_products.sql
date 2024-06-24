{{config(materialized='table')}}

with dim_products as (
	select 
		row_number() over(order by product asc) as product_sk_id,
		row_number() over(order by product asc) as product_id,
		product,
		price_each::numeric(10,2) as price_each
	from (select distinct product, price_each
	from  {{ref('final_product_sales')}}
	) distinct_product
)
select *
from dim_products