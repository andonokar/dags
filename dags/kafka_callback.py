import json
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
import pickle


def producer_function(text):
    return None, text


def produce_to_kafka(context):
    output = json.dumps(pickle.loads(pickle.dumps(context)))

    producer = ProduceToTopicOperator(
        kafka_config_id="kafka_producer_1",
        task_id='produce_to_topic',
        topic="airflow_logs",
        producer_function=producer_function,
        producer_function_args=output
    )
    producer.execute(pickle.loads(pickle.dumps(context)))
