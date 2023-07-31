### Airflow DAG ###

import pendulum

from airflow import DAG
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from dateutil import parser
from datetime import datetime, timedelta

default_args = {
    'owner': 'jmontenaro',
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2016, 1, 1, tz="Europe/Amsterdam")
}

combined = DAG (
    'combined-pipeline-demo',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
)

spark_pi_step = CDEJobRunOperator (
    task_id='spark_pi',
    dag=combined,
    job_name='jmontenaro_combined_spark_pi'
)

cdw_query = """show databases;"""

show_databases_step = CDWOperator (
    task_id="show_databases",
    dag=combined,
    cli_conn_id="default-hive-aws",
    hql=cdw_query,
    schema='default',
    use_proxy_user=False,
    query_isolation=True
)

spark_pi_step >> show_databases_step