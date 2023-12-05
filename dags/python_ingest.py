from airflow import DAG
from datetime import datetime
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import json
from airflow.providers.cncf.kubernetes.secret import Secret

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

aws_key = Secret(deploy_type="env", deploy_target="AWS_ACCESS_KEY_ID", secret="aws-secret", key="AWS_ACCESS_KEY_ID")
secret_aws_key = Secret(deploy_type="env", deploy_target="AWS_SECRET_ACCESS_KEY", secret="aws-secret", key="AWS_SECRET_ACCESS_KEY")

with DAG(
    'python_ingest',
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
            bucket_name = val.get("bucket_name")
            return bucket_name

    def wait_for_event(message, **context):
        bucket_name = message.get("bucket_name")
        KubernetesPodOperator(
            namespace="python",
            image=f"915484175192.dkr.ecr.us-east-1.amazonaws.com/dr_pythoningest:1.0",
            name=f"{bucket_name}_pythoningest",
            random_name_suffix=True,
            cmds=["python3", "e2etest.py"],
            arguments=[bucket_name],
            get_logs=True,
            in_cluster=True,
            secrets=[aws_key, secret_aws_key]
        ).execute(context)

    kafka_task = AwaitMessageTriggerFunctionSensor(
        kafka_config_id='kafka_consumer_1',
        task_id='test_kafka',
        topics=['python_ingest'],
        apply_function="kafka_listener_test.await_function",
        event_triggered_function=wait_for_event
    )
