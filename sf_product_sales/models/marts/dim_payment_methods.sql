
{{config(materialized='table')}}

with dim_payment_methods as (
	select
		row_number() over(order by payment_method asc) as payment_method_id,
		payment_method
	from (
		select distinct payment_method
		from {{ref('stg_product_sales')}}
	) distinct_payment_method
)

select *
from dim_payment_methods