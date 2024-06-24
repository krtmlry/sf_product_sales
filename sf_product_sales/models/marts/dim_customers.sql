{{ config(materialized='table')}}

with stg_dim_customers as (
	select
		row_number() over(order by purchase_address asc) customer_sk_id,
		row_number() over(order by purchase_address asc) customer_id,
		purchase_address,
		ltrim(split_part(purchase_address,',',2))::varchar as city,
		split_part(ltrim(split_part(purchase_address,',',3)),' ',1)::varchar as cust_state_abbrv
	from ( select distinct purchase_address
	from {{ref('final_product_sales')}} ) distinct_address
),
	dim_customers as (
		select
		a.customer_sk_id,
		a.customer_id,
		a.purchase_address,
		a.city,
		a.cust_state_abbrv,
		b.state_name as state_name
		from stg_dim_customers as a
		left join dim_states as b on a.cust_state_abbrv = b.state_abbrv
)

select *
from dim_customers