from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow import DAG
from datetime import datetime
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'csn_demanda',
    default_args=default_args,
    schedule_interval=None,  # Set to None if you don't want the DAG to be scheduled
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    demanda_bronze = SparkKubernetesOperator(
        task_id='demanda_bronze',
        application_file='demanda_bronze.yaml',
        namespace="spark-operator",
        watch=True
    )
    demanda_silver = SparkKubernetesOperator(
        task_id='demanda_silver',
        application_file='demanda_silver.yaml',
        namespace="spark-operator",
        watch=True,
        trigger_rule="none_skipped"
    )
    demanda_bronze >> demanda_silver

with DAG(
    'csn_carteira',
    default_args=default_args,
    schedule_interval=None,  # Set to None if you don't want the DAG to be scheduled
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False
) as dag2:
    carteira_bronze = SparkKubernetesOperator(
        task_id='carteira_bronze',
        application_file='carteira_bronze.yaml',
        namespace="spark-operator",
        watch=True
    )
    carteira_silver = SparkKubernetesOperator(
        task_id='carteira_silver',
        application_file='carteira_silver.yaml',
        namespace="spark-operator",
        watch=True,
        trigger_rule="none_skipped"
    )
    carteira_bronze >> carteira_silver


