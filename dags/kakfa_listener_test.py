from airflow import DAG
from datetime import datetime
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
import uuid

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'my_kafka_dagv2',
    default_args=default_args,
    schedule_interval="@continuous",  # Set to None if you don't want the DAG to be scheduled
    max_active_runs=1
) as dag:
    def await_function(message):
        print(message)
        try:
            val = json.loads(message.value())
        except Exception as err:
            print(type(err).__name__ + ': ' + str(err))
        else:
            val.get("csn")
            return val

    def wait_for_event(message, **context):
        TriggerDagRunOperator(
            trigger_dag_id=message,
            task_id=f"triggered_downstream_dag_{uuid.uuid4()}",
        ).execute(context)

    kafka_task = AwaitMessageTriggerFunctionSensor(
        task_id='test_kafka',
        topics=['teste'],
        apply_function="kakfa_listener_test.await_function",
        event_triggered_function=wait_for_event
    )
