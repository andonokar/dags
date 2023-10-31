from airflow import DAG
from datetime import datetime
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor
import json

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'my_kafka_dagv2',
    default_args=default_args,
    schedule_interval=None,  # Set to None if you don't want the DAG to be scheduled
    max_active_runs=1
) as dag:
    def await_function(message):
        val = json.loads(message.value())
        print(val) 
        return val

    def wait_for_event(message, **context):
        print(message)
        print(context)
        return message, context

    kafka_task = AwaitMessageTriggerFunctionSensor(
        task_id='test_kafka',
        topics=['teste'],
        apply_function="kakfa_listener_test.await_function",
        event_triggered_function=wait_for_event
    )
