
{{config(materialized='table')}}


with dim_stores as (
	select
	row_number() over(order by store asc) as store_id,
	store
	from (
		select distinct store
		from {{ref('stg_fact_sales')}} ) distinct_stores
)

select *
from dim_stores