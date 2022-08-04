import datetime
import os
import re
from io import StringIO

import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow_clickhouse_plugin.operators.clickhouse_operator import \
    ClickHouseOperator
from clickhouse import tables

LOCAL_PATH = os.environ.get('AIRFLOW_HOME')


@dag(
    dag_id='example_dag',
    description='Example ETL',
    start_date=datetime.datetime.now(),
    schedule_interval='0 2 * * *',
)
def info_oil_fuels_etl():

    @task(task_id='extract')
    def extract():
        with open(f'{LOCAL_PATH}/dags/data/bfro_reports.json', 'rb') as file:
            data = file.read()
            return data.decode('utf-8')

    @task(task_id='transform')
    def transform(reports):
        def apply_normalize_year(row):
            numbers_regex = '[0-9]{4}'
            numbers = re.findall(numbers_regex, row)
            return numbers[0] if numbers else '0'

        def apply_normalize_id(row):
            try:
                 int(row)
            except ValueError:
                return np.NAN
            return row

        df = pd.read_json(StringIO(reports), orient='records', dtype=str, lines=True)

        df['YEAR'] = df['YEAR'].apply(apply_normalize_year)
        df['REPORT_NUMBER'] = df['REPORT_NUMBER'].apply(apply_normalize_id)
        df.drop(
            columns=['OBSERVED', 'ALSO_NOTICED', 'OTHER_WITNESSES', 'OTHER_STORIES',
                     'LOCATION_DETAILS', 'A_&_G_References', 'ENVIRONMENT', 'TIME_AND_CONDITIONS'],
            inplace=True,
        )
        df.replace(' ', '', regex=True, inplace=True)
        df.replace("'", '', regex=True, inplace=True)
        # print('DF: \n' + df.to_string())
        df.dropna(subset=('REPORT_NUMBER', ), inplace=True)
        df.fillna('', inplace=True)
        return ','.join(map(str, df.to_records(index=False).tolist()))

    create_tables = ClickHouseOperator(
        task_id='create_tables',
        clickhouse_conn_id='clickhouse_connection_1',
        sql=tables.CREATE_BFRO_TABLE,
        database='airflow',
    )

    load = ClickHouseOperator(
        task_id='load',
        clickhouse_conn_id='clickhouse_connection_1',
        sql="INSERT INTO BFRO(* EXCEPT(timestamp)) VALUES {{ ti.xcom_pull(task_ids='transform', key='return_value') }}",
        database='airflow',
    )

    create_tables >> transform(extract())
    create_tables >> load




dag = info_oil_fuels_etl()
