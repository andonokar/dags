import json
import requests
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
import time


def producer_function(*args, **kwargs):
    yield None, kwargs['text']


def produce_to_kafka(context):
    ti = context["ti"]
    output = {
        "dag_id": str(ti.dag_id),
        "task_id": str(ti.task_id),
        "run_id": str(ti.run_id),
        "start_date": str(ti.start_date),
        "end_date": str(ti.end_date),
        "duration": str(ti.duration),
        "try_number": str(ti.try_number),
        "state": str(ti.state),
        "log_url": str(ti.log_url)
    }
    time.sleep(600)
    logs = []
    log_url = f'http://airflow-webserver:8080/api/v1/dags/{ti.dag_id}/dagRuns/{ti.run_id}/taskInstances/{ti.task_id}/logs/{ti.try_number}?full_content=true'
    # params = {"full_content": True}
    response = requests.get(log_url, auth=("admin", "admin"))
    for line in response.iter_lines():
        decoded_line = line.decode('utf-8')
        logs.append(decoded_line)
    # if response.status_code == 200:
    # logs = response.text
    output['logs'] = logs

    json_output = json.dumps(output)
    producer = ProduceToTopicOperator(
        kafka_config_id="kafka_producer_1",
        task_id='produce_to_topic',
        topic="airflow_logs",
        producer_function=producer_function,
        producer_function_kwargs={"text": json_output}
    )
    producer.execute(context)
