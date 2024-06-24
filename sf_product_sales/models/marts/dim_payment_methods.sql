
{{config(materialized='table')}}

with dim_payment_methods as (
	select
		row_number() over(order by payment_method) as pay_method_sk_id,
		row_number() over(order by payment_method) as pay_method_id,
		payment_method
	from (
		select distinct payment_method
		from {{ref('final_product_sales')}}
	) distinct_payment_method
)

select *
from dim_payment_methods