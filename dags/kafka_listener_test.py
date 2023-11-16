from airflow import DAG
from datetime import datetime
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'my_kafka_dagv5',
    default_args=default_args,
    schedule_interval="@continuous",  # Set to None if you don't want the DAG to be scheduled
    max_active_runs=1,
    catchup=False,
    # is_paused_upon_creation=False
) as dag:
    def await_function(message):
        try:
            val = json.loads(message.value())
        except Exception as err:
            print(type(err).__name__ + ': ' + str(err))
            raise err
        else:
            table_name = val.get("table_name")
            return table_name

    def wait_for_event(message, **context):
        table_name = message.get("table_name")
        TriggerDagRunOperator(
            trigger_dag_id=table_name,
            task_id=f"triggered_downstream_dag_{table_name}",
        ).execute(context)

    kafka_task = AwaitMessageTriggerFunctionSensor(
        kafka_config_id='kafka_consumer_1',
        task_id='test_kafka',
        topics=['csn'],
        apply_function="kafka_listener_test.await_function",
        event_triggered_function=wait_for_event
    )
