import datetime
import os

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.operators.clickhouse_operator import \
    ClickHouseOperator
from clickhouse import tables
from clickhouse.converters import get_count_days

LOCAL_PATH = os.environ.get('AIRFLOW_HOME')


@dag(
    dag_id='example_dag',
    description='Example ETL',
    start_date=datetime.datetime.now(),
    schedule_interval='0 2 * * *',
)
def example_etl_dag():

    @task(task_id='extract_orders_1')
    def extract_orders():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_connection_1')
        return postgres_hook.get_records("SELECT * FROM orders;")

    @task(task_id='extract_customers_1')
    def extract_customers():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_connection_1')
        return postgres_hook.get_records("SELECT * FROM customers;")

    @task(task_id='transform_1')
    def transform(orders, customers):

        orders_df = pd.DataFrame.from_records(orders)
        orders_df.columns = (
            'order_id', 'customer_id', 'order_total_usd', 'make', 'model', 'delivery_city',
            'delivery_company', 'delivery_address', 'created_at',
        )

        customers_df = pd.DataFrame.from_records(customers)
        customers_df.columns = ('customer_id', 'customer_name')

        df_to_load = orders_df.merge(customers_df, on='customer_id')
        df_to_load['created_at'] = df_to_load['created_at'].apply(get_count_days)
        df_to_load.replace("'", "", inplace=True, regex=True)

        return ','.join(map(str, df_to_load.to_records(index=False).tolist()))

    create_tables = ClickHouseOperator(
        task_id='ch_create_tables_1',
        clickhouse_conn_id='clickhouse_connection_1',
        sql=tables.CH_CREATE_ORDER_TABLE,
        database='airflow',
    )

    load = ClickHouseOperator(
        task_id='ch_load_datamart_1',
        clickhouse_conn_id='clickhouse_connection_1',
        sql="INSERT INTO orders_datamart(* EXCEPT(timestamp)) "
            "VALUES {{ ti.xcom_pull(task_ids='transform_1', key='return_value') }}",
        database='airflow',
    )

    create_tables >> transform(extract_orders(), extract_customers()) >> load


dag = example_etl_dag()
