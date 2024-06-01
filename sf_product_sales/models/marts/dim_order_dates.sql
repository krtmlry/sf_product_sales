{{config(materialized='table')}}

with dim_order_dates as (
	select
		row_number() over(order by order_date asc) as orderdate_sk_id,
		order_date as order_time_stamp,
		date(order_date) as date,
		extract(year from order_date) as year,
		extract(month from order_date) as month,
		extract(day from order_date) as day,
		extract(dow from order_date) as dow,
		extract(hour from order_date) as hour,
		extract(minute from order_date) as minute,
		monthname(order_date) as month_name,
		dayname(order_date) as day_name
	from (select
		distinct order_date
	from {{ref('stg_fact_sales')}}) distinct_order_date
)

select *
from dim_order_dates
