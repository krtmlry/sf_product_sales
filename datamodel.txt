dim_customers
---
customer_sk_id int
purchase_address varchar
city varchar

dim_products
---
product_id int
product varchar
price numeric

dim_stores
---
store_id int
store varchar

dim_payment_methods
---
payment_method_id int
payment_method varchar

dim_order_dates
---
orderdate_sk_id serial
order_time_stamp timestamp
date date
year numeric
month numeric
day numeric
dow numeric
hour numeric
minute numeric
month_name varchar
day_name varchar

final_fact_sales
----
order_sk_id int
order_id int
customer_id int FK >- dim_customers.customer_sk_id
product_id int FK >- dim_products.product_id
price_each numeric
qty_ordered int
total_price numeric
orderdate_id int FK >- dim_order_dates.orderdate_sk_id
purchase_address_id int
city varchar
store_id int FK >- dim_stores.store_id
payment_method_id int FK >- dim_payment_methods.payment_method_id

