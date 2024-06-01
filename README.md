# ELT Data pipeline using Prefect, DBT and Snowflake

## About
This pipeline will extract data from a csv file using `python`, load it into a staging table in `snowflake` and apply some transformations using `dbt`. The whole process will be orchestrated by `prefect`.

This is an updated version of a previous ETL project. Reason for the update can be read [here](link).

The postgres version can be seen [here](https://github.com/krtmlry/prefect-dbt-postgres).

---
## Files

`sf_product_sales` - contains all dbt models for transformations.

`prefect_flows` - contains the python script for the entire process.

`img` - contains image files.

`Dataset` used for this project can be seen [here](https://github.com/krtmlry/datasets/tree/main/merged_sales_csv)

---
## Project diagram
![proj-diagram](https://github.com/krtmlry/sf_product_sales/blob/main/img/proj-diagram.png)

Process overview:
1. Data will be extracted using python from a local csv file.
2. Data will then be loaded into a staging table in snowflake.
3. After loading, dbt models will be run to perform transformations.
4. Data will then be loaded to marts after the transformation for user consumption.

---

## Data model diagram

![data-model](https://github.com/krtmlry/sf_product_sales/blob/main/img/proj-diagram.png)

----
