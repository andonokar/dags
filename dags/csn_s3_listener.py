from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from datetime import datetime
import json

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0
}


def producer_function(*args, **kwargs):
    template = {"bucket_name": "csn-ingestion-layer-prd-9154-8417-5192"}
    kafka_msg = json.dumps(template)
    yield None, kafka_msg


with DAG(
        'csn_start_trigger',
        default_args=default_args,
        schedule_interval="@continuous",  # Set to None if you don't want the DAG to be scheduled
        max_active_runs=1,
        catchup=False
) as dag:
    unchanged_sensor = S3KeysUnchangedSensor(
        task_id='csn_sensor',
        aws_conn_id="aws_csn",
        bucket_name='csn-ingestion-layer-prd-9154-8417-5192',
        prefix='',
        inactivity_period=300,
        min_objects=22
    )

    trigger_dag_run = ProduceToTopicOperator(
        task_id='trigger_dag_on_success',
        topic="python_ingest",
        producer_function=producer_function
    )

unchanged_sensor >> trigger_dag_run
