	-- materialized view
{{config(materialized='view')}}

with mart_product_sales as (
	select
	a.order_id,
	c.customer_id as customer_id,
	b.product,
	b.price_each,
	a.qty_ordered,
	a.total_price,
	f.order_time_stamp,
	f.day_name,
	f.dow as day_name_num,
	f.month_name,
	f.month as month_num,
	c.purchase_address,
	c.city,
	c.state_name,
	d.store,
	e.payment_method
	from {{ref('final_fact_sales')}} as a
	left join {{ref('dim_products')}} as b on a.product_sk_id = b.product_sk_id
	left join {{ref('dim_customers')}} as c on a.customer_sk_id = c.customer_sk_id
	left join {{ref('dim_stores')}} as d on a.store_sk_id = d.store_sk_id
	left join {{ref('dim_payment_methods')}} as e on a.pay_method_sk_id = e.pay_method_sk_id
	left join {{ref('dim_order_dates')}} as f on a.orderdate_sk_id = f.orderdate_sk_id
)

select *
from mart_product_sales