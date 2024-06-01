-- to remove nulls and repeating headers

{{ config(materialized='view') }}

with rm_nulls_dups as (
	select distinct *
	from {{ source('raw_source','RAW_SALES_LANDING') }}
	where "Order ID" != 'Order ID' and "Order ID" is not null
)

select *
from rm_nulls_dups