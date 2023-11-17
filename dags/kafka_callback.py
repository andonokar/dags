import json
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator


def producer_function(text):
    return None, text


def produce_to_kafka(context):
    output = dict([(key, context[key]) for key in context])
    raise KeyError(output, str(output))
    arg = json.dumps(output)

    producer = ProduceToTopicOperator(
        kafka_config_id="kafka_producer_1",
        task_id='produce_to_topic',
        topic="airflow_logs",
        producer_function=producer_function,
        producer_function_args=arg
    )
    producer.execute(output)
