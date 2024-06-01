{{config(materialized='view')}}

with t_cast_types as (
	select
		cast("Order ID" as int) as order_id,
		cast("Product" as varchar) as product,
		cast("Price Each" as numeric(10,2)) as price_each,
		cast("Quantity Ordered" as int) as qty_ordered,
        case
            when length("Order Date") = 14 then to_timestamp("Order Date", 'MM/DD/YY HH24:MI')
            else to_timestamp("Order Date", 'MM/DD/YYYY HH24:MI')
        end as order_date,
		cast("Purchase Address" as varchar) as purchase_address,
		cast("Store" as varchar) as store,
		cast("Payment Method" as varchar) as payment_method
	from {{ref('t_rm_nulls_dups')}}
)

select *
from t_cast_types