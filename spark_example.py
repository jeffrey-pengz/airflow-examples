import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk"
}

dag = DAG(dag_id='spark_examples',
          default_args=default_args,
          catchup=False,
          schedule_interval=None)

run_pi = SparkSubmitOperator(task_id='run_pi',
                             conn_id='spark_k8s',
                             application=f'./spark/pi.py',
                             name='run_pi',
                             dag=dag
                             )


run_pi