import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os
from prefect import task, flow
from dbt.cli.main import dbtRunner, dbtRunnerResult
load_dotenv()

@task(name='extract-csv')
def get_csv():
    merged_csv_path = os.getenv('merged_csv_path')
    df = pd.read_csv(merged_csv_path)
    return df

@task(name='init-sf-credentials')
def sf_credentials():
    sf_creds = {
        'n_sf_user': os.getenv('n_sf_user'),
        'n_sf_pass' : os.getenv('n_sf_pass'),
        'n_sf_account_id' : os.getenv('n_sf_account_id'),
        'n_sf_warehouse' : os.getenv('n_sf_warehouse'),
        'n_sf_db' : os.getenv('n_sf_db'),
        'n_sf_schema' : os.getenv('n_sf_schema'),
        'n_sf_tbl_name' : os.getenv('n_sf_tbl_name'),
        'n_sf_role' : os.getenv('n_sf_role')
    }
    return sf_creds

@task(name='load-csv-to-snowflake')
def load_csv(df,sf_creds):    
    conn = snowflake.connector.connect(
        user=sf_creds['n_sf_user'],
        password=sf_creds['n_sf_pass'],
        account=sf_creds['n_sf_account_id'],
        warehouse=sf_creds['n_sf_warehouse'],
        database=sf_creds['n_sf_db'],
        schema=sf_creds['n_sf_schema']
    )
    conn.cursor().execute(f"use role {sf_creds['n_sf_role']}")
    conn.cursor().execute(f"use schema {sf_creds['n_sf_schema']}")
    write_pandas(conn, df, sf_creds['n_sf_tbl_name'], schema=sf_creds['n_sf_schema'], auto_create_table=True)
    conn.close()

@task(name='run-dbt-models')
def run_dbt():
    dbt_sf_product_sales_dir = os.getenv('dbt_sf_product_sales_dir')
    dbt_profiles_dir = os.getenv('dbt_profiles')
    dbt = dbtRunner()

    cli_args_1 = ["run", "-m", "tag:p1", "--project-dir", dbt_sf_product_sales_dir, "--profiles-dir", dbt_profiles_dir]
    cli_args_2 = ["run", "-m", "tag:p2", "--project-dir", dbt_sf_product_sales_dir, "--profiles-dir", dbt_profiles_dir]

    res_1 = dbt.invoke(cli_args_1)
    res_2 = dbt.invoke(cli_args_2)

    for r in res_1.result:
        print(f"{r.node.name}: {r.status}")
        
    for r in res_2.result:
        print(f"{r.node.name}: {r.status}")


@flow(name='elt-main-snowflake')
def elt_main():
    df = get_csv()
    sf_creds = sf_credentials()
    load_csv(df,sf_creds)
    run_dbt()
    
if __name__ == '__main__':
    elt_main()