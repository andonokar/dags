import json
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator


def producer_function(*args, **kwargs):
    return None, kwargs['text']


def produce_to_kafka(context):
    ti = context["ti"]
    output = json.dumps({
        "dag_id": str(ti.dag_id),
        "task_id": str(ti.task_id),
        "run_id": str(ti.run_id),
        "start_date": str(ti.start_date),
        "end_date": str(ti.end_date),
        "duration": str(ti.duration),
        "log_url": str(ti.log_url)
    })

    producer = ProduceToTopicOperator(
        kafka_config_id="kafka_producer_1",
        task_id='produce_to_topic',
        topic="airflow_logs",
        producer_function=producer_function,
        producer_function_kwargs={"text": output}
    )
    producer.execute(context)
