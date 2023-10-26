from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow import DAG
from datetime import datetime

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'my_spark_dag',
    default_args=default_args,
    schedule_interval=None,  # Set to None if you don't want the DAG to be scheduled
) as dag:
    spark_task = SparkKubernetesOperator(
        task_id='test_spark',
        application_file='spark.yaml',
        namespace="spark-operator",
        watch=True
    )
    task_deleter = BashOperator(
        task_id='delete_test_spark',
        bash_command="kubectl delete -n spark-operator spark.yaml",
        trigger_rule='all_done'
    )
    spark_task >> task_deleter
