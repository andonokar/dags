from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.operators.resource import KubernetesDeleteResourceOperator
from airflow import DAG
from datetime import datetime
yaml_conf = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: domrock-spark-teste
  namespace: spark-operator
spec:
  volumes:
    - name: ivy
      emptyDir: {}
  sparkConf:
    spark.driver.extraJavaOptions: "-Divy.cache.dir=/tmp -Divy.home=/tmp, -Dcom.amazonaws.services.s3.enableV4=true"
    spark.executor.extraJavaOptions: "-Dcom.amazonaws.services.s3.enableV4=true"
    spark.kubernetes.allocation.batch.size: "10"
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.databricks.delta.schema.autoMerge.enabled: "true"
  hadoopConf:
    fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
    fs.s3a.path.style.access: "True"
  type: Python
  pythonVersion: "3"
  image: "andonokar/testespark:1.1"
  imagePullPolicy: Always
  imagePullSecrets:
    - docker-pass
  mainApplicationFile: "local:///app/e2etest.py"
  arguments:
    - "csn"
    - "DEMANDA"
    - "bronze"
  sparkVersion: "3.3.1"
  restartPolicy:
    type: Never
  driver:
#    envSecretKeyRefs:
#      AWS_ACCESS_KEY_ID:
#        name: aws-secret
#        key: awsAccessKeyId
#      AWS_SECRET_ACCESS_KEY:
#        name: aws-secret
#        key: awsSecretAccessKey
    cores: 1
    coreRequest: "500m"
    coreLimit: "1200m"
    memory: "4g"
    labels:
      version: 3.3.2
    volumeMounts:
      - name: ivy
        mountPath: /tmp
  executor:
#    envSecretKeyRefs:
#      AWS_ACCESS_KEY_ID:
#        name: aws-secret
#        key: awsAccessKeyId
#      AWS_SECRET_ACCESS_KEY:
#        name: aws-secret
#        key: awsSecretAccessKey
    cores: 1
    coreRequest: "500m"
    instances: 6
    memory: "4g"
    labels:
      version: 3.3.1
    volumeMounts:
      - name: ivy
        mountPath: /tmp
"""

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'my_spark_dag',
    default_args=default_args,
    schedule_interval=None,  # Set to None if you don't want the DAG to be scheduled
) as dag:
    spark_task = SparkKubernetesOperator(
        task_id='test_spark',
        application_file=yaml_conf,
        namespace="spark-operator",
        watch=True
    )
    task_deleter = KubernetesDeleteResourceOperator(
        task_id='delete_test_spark',
        yaml_conf=yaml_conf,
        namespace='spark-operator',
        trigger_rule='all_done'
    )
    spark_task >> task_deleter
