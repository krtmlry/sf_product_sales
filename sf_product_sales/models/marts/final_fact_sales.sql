{{config(materialized='table')}}

with final_fact_sales as (
	select
	a.order_sk_id,
	a.order_id,
	c.customer_sk_id,
	b.product_sk_id,
	a.qty_ordered,
	(a.qty_ordered * b.price_each)::numeric(10,2) as total_price,
	f.orderdate_sk_id,
	d.store_sk_id,
	e.pay_method_sk_id
	
	from {{ref('final_product_sales')}} as a
	left join {{ref('dim_products')}} as b on a.product = b.product
	left join {{ref('dim_customers')}} as c on a.purchase_address = c.purchase_address
	left join {{ref('dim_stores')}} as d on a.store = d.store
	left join {{ref('dim_payment_methods')}} as e on a.payment_method = e.payment_method
	left join {{ref('dim_order_dates')}} as f on a.order_date = f.order_time_stamp
	order by order_sk_id asc
)

select *
from final_fact_sales

