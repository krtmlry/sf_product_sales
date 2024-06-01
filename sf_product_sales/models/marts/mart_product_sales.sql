	-- materialized view
{{config(materialized='view')}}

with mart_product_sales as (
	select
	--order_sk_id, --optional
	a.order_id,
	c.customer_sk_id as customer_id,
	b.product,
	b.price_each,
	a.qty_ordered,
	a.total_price::numeric(10,2) as total_price,
	f.order_time_stamp,
	f.day_name,
	f.month_name,
	c.purchase_address,
	c.city,
	d.store,
	e.payment_method
	from {{ref('final_fact_sales')}} as a
	left join {{ref('dim_products')}} as b on a.product_id = b.product_id
	left join {{ref('dim_customers')}} as c on a.purchase_address_id = c.customer_sk_id
	left join {{ref('dim_stores')}} as d on a.store_id = d.store_id
	left join {{ref('dim_payment_methods')}} as e on a.payment_method_id = e.payment_method_id
	left join {{ref('dim_order_dates')}} as f on a.orderdate_id = f.orderdate_sk_id
)

select *
from mart_product_sales