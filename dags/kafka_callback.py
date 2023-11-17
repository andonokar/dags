import json
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator


def producer_function(text):
    return None, text


def produce_to_kafka(context):
    ti = context["ti"]
    output = json.dumps({
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "start_date": ti.start_date,
        "end_date": ti.end_date,
        "duration": ti.duration,
        "log_url": ti.log_url
    })

    producer = ProduceToTopicOperator(
        kafka_config_id="kafka_producer_1",
        task_id='produce_to_topic',
        topic="airflow_logs",
        producer_function=producer_function,
        producer_function_args=output
    )
    producer.execute(context)
