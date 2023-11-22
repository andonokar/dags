import json
import requests
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator


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
        "state": str(ti.state),
        "log_url": str(ti.log_url)
    }

    log_url = f'http://localhost:8080/api/v1/dags/{ti.dag_id}/tasks/{ti.task_id}/logs?try_number=0&limit=1000'
    response = requests.get(log_url)
    if response.status_code == 200:
        logs = response.content.decode('utf-8')
        output['logs'] = logs
    else:
        output['logs'] = ''

    json_output = json.dumps(output)
    producer = ProduceToTopicOperator(
        kafka_config_id="kafka_producer_1",
        task_id='produce_to_topic',
        topic="airflow_logs",
        producer_function=producer_function,
        producer_function_kwargs={"text": json_output}
    )
    producer.execute(context)
