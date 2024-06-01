{{config(materialized='table')}}

with final_fact_sales as (
	select
	a.order_sk_id,
	a.order_id,
	c.customer_sk_id as customer_id,
	b.product_id,
	b.price_each,
	a.qty_ordered,
	a.total_price::numeric(10,2) as total_price,
	f.orderdate_sk_id as orderdate_id,
	c.customer_sk_id as purchase_address_id,
	c.city,
	d.store_id,
	e.payment_method_id
	
	from {{ref('stg_fact_sales')}} as a
	left join {{ref('dim_products')}} as b on a.product = b.product
	left join {{ref('dim_customers')}} as c on a.purchase_address = c.purchase_address
	left join {{ref('dim_stores')}} as d on a.store = d.store
	left join {{ref('dim_payment_methods')}} as e on a.payment_method = e.payment_method
	left join {{ref('dim_order_dates')}} as f on a.order_date = f.order_time_stamp
	order by order_sk_id asc
)

select *
from final_fact_sales

