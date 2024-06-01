# Reason for updating the project
---

This old etl pipeline [project]() of mine was my first time creating an end-to-end etl pipeline and I was only following a video tutorial.

There are some mistakes on that project particularly on the data modelling part.

---

## Data modelling problem

After all data cleaning process was done. I created dimension tables for customers, order dates and products.

The dim tables should just contain unique values for customers, order dates and products but instead I treated all rows as unique values and created a surrogate key column to make a temporary primary key to be used for joining the dim tables to the fact tables.

So after the entire process finished, I now have 3 dimension tables that has the same number of rows with my fact table.

See pictures below for a better overview:

`customer_dim`
![customer_dim]()

`product_dim`
![product_dim]()

`datetime_dim`
![datetime_dim]()

`order_details` - fact table
![order_details]()

