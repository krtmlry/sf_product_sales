
{{config(materialized='table')}}


with dim_stores as (
	select
	row_number() over(order by store asc) as store_sk_id,
	row_number() over(order by store asc) as store_id,
	store
	from (
		select distinct store
		from {{ref('final_product_sales')}}
	) distinct_stores
)

select *
from dim_stores