from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow import DAG
from datetime import datetime

default_args = {
    'owner': 'anderson',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

template = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: {{{{ "{table}" | lower | replace("_", "-") }}}}-{{{{ ts_nodash | lower }}}}
  namespace: spark-operator
spec:
  volumes:
    - name: ivy
      emptyDir: {{}}
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
  image: "andonokar/testespark:1.2"
  imagePullPolicy: Always
  imagePullSecrets:
    - docker-pass
  mainApplicationFile: "local:///app/e2etest.py"
  arguments:
    - "csn"
    - {table}
    - {setup}
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
    memory: "2g"
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
    instances: 2
    memory: "4g"
    labels:
      version: 3.3.1
    volumeMounts:
      - name: ivy
        mountPath: /tmp

"""

with DAG(
    'csn',
    default_args=default_args,
    schedule_interval=None,  # Set to None if you don't want the DAG to be scheduled
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    demanda_bronze = SparkKubernetesOperator(
        task_id='demanda_bronze',
        application_file=template.format(table='DEMANDA', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    demanda_silver = SparkKubernetesOperator(
        task_id='demanda_silver',
        application_file=template.format(table='DEMANDA', setup='silver'),
        namespace="spark-operator",
        watch=True,
    )

    carteira_bronze = SparkKubernetesOperator(
        task_id='carteira_bronze',
        application_file=template.format(table='CARTEIRA', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    carteira_silver = SparkKubernetesOperator(
        task_id='carteira_silver',
        application_file=template.format(table='CARTEIRA', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    SKU_CLASSIFICACAO_bronze = SparkKubernetesOperator(
        task_id='SKU_CLASSIFICACAO_bronze',
        application_file=template.format(table='SKU_CLASSIFICACAO', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    SKU_CLASSIFICACAO_silver = SparkKubernetesOperator(
        task_id='SKU_CLASSIFICACAO_silver',
        application_file=template.format(table='SKU_CLASSIFICACAO', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    MB51_bronze = SparkKubernetesOperator(
        task_id='MB51_bronze',
        application_file=template.format(table='MB51', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    MB51_silver = SparkKubernetesOperator(
        task_id='MB51_silver',
        application_file=template.format(table='MB51', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    ZSDI19_bronze = SparkKubernetesOperator(
        task_id='ZSDI19_bronze',
        application_file=template.format(table='ZSDI19', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    ZSDI19_silver = SparkKubernetesOperator(
        task_id='ZSDI19_silver',
        application_file=template.format(table='ZSDI19', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    TRANSACAO_ESTOQUE_bronze = SparkKubernetesOperator(
        task_id='TRANSACAO_ESTOQUE_bronze',
        application_file=template.format(table='TRANSACAO', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    TRANSACAO_ESTOQUE_silver = SparkKubernetesOperator(
        task_id='TRANSACAO_ESTOQUE_silver',
        application_file=template.format(table='TRANSACAO', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    ZV36_bronze = SparkKubernetesOperator(
        task_id='ZV36_bronze',
        application_file=template.format(table='ZV36', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    ZV36_silver = SparkKubernetesOperator(
        task_id='ZV36_silver',
        application_file=template.format(table='ZV36', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    FATURA_bronze = SparkKubernetesOperator(
        task_id='FATURA_bronze',
        application_file=template.format(table='FATURA', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    FATURA_silver = SparkKubernetesOperator(
        task_id='FATURA_silver',
        application_file=template.format(table='FATURA', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    ESTOQUE_bronze = SparkKubernetesOperator(
        task_id='ESTOQUE_bronze',
        application_file=template.format(table='ESTOQUE', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    ESTOQUE_silver = SparkKubernetesOperator(
        task_id='ESTOQUE_silver',
        application_file=template.format(table='ESTOQUE', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_COMBO_bronze = SparkKubernetesOperator(
        task_id='DEPARA_COMBO_bronze',
        application_file=template.format(table='DEPARA_COMBO', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    DEPARA_COMBO_silver = SparkKubernetesOperator(
        task_id='DEPARA_COMBO_silver',
        application_file=template.format(table='DEPARA_COMBO', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_NORMAS_bronze = SparkKubernetesOperator(
        task_id='DEPARA_NORMAS_bronze',
        application_file=template.format(table='DEPARA_NORMAS', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )
    DEPARA_NORMAS_silver = SparkKubernetesOperator(
        task_id='DEPARA_NORMAS_silver',
        application_file=template.format(table='DEPARA_NORMAS', setup='silver'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_FILIAL = SparkKubernetesOperator(
        task_id='DEPARA_FILIAL',
        application_file=template.format(table='DEPARA_FILIAL', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_GERENCIA = SparkKubernetesOperator(
        task_id='DEPARA_GERENCIA',
        application_file=template.format(table='DEPARA_GERENCIA', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_TIPOAVALIACAO = SparkKubernetesOperator(
        task_id='DEPARA_TIPOAVALIACAO',
        application_file=template.format(table='DEPARA_TIPOAVALIACAO', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_CLIENTE = SparkKubernetesOperator(
        task_id='DEPARA_CLIENTE',
        application_file=template.format(table='DEPARA_CLIENTE', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_CLASSIFICACAO = SparkKubernetesOperator(
        task_id='DEPARA_CLASSIFICACAO',
        application_file=template.format(table='DEPARA_CLASSIFICACAO', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_PRODUTO = SparkKubernetesOperator(
        task_id='DEPARA_PRODUTO',
        application_file=template.format(table='DEPARA_PRODUTO', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_REGIAO = SparkKubernetesOperator(
        task_id='DEPARA_REGIAO',
        application_file=template.format(table='DEPARA_REGIAO', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )

    DEPARA_MOVIMENTACAO = SparkKubernetesOperator(
        task_id='DEPARA_MOVIMENTACAO',
        application_file=template.format(table='DEPARA_MOVIMENTACAO', setup='bronze'),
        namespace="spark-operator",
        watch=True
    )

    gold_operations_demanda_inicial = SparkKubernetesOperator(
        task_id='gold_operations_demanda_inicial',
        application_file=template.format(table='gold_operations_demanda_inicial', setup='gold'),
        namespace="spark-operator",
        watch=True
    )

    gold_operations_estoque = SparkKubernetesOperator(
        task_id='gold_operations_estoque',
        application_file=template.format(table='gold_operations_estoque', setup='gold'),
        namespace="spark-operator",
        watch=True
    )

    gold_operations_faturamento_serie_temporal = SparkKubernetesOperator(
        task_id='gold_operations_faturamento_serie_temporal',
        application_file=template.format(table='gold_operations_faturamento_serie_temporal', setup='gold'),
        namespace="spark-operator",
        watch=True
    )

    gold_operations_predicao = SparkKubernetesOperator(
        task_id='gold_operations_predicao',
        application_file=template.format(table='gold_operations_predicao', setup='gold'),
        namespace="spark-operator",
        watch=True
    )
    tasks = [DEPARA_COMBO_silver, DEPARA_NORMAS_silver, DEPARA_FILIAL, DEPARA_GERENCIA, DEPARA_TIPOAVALIACAO, DEPARA_CLIENTE,
             DEPARA_CLASSIFICACAO, DEPARA_PRODUTO, DEPARA_REGIAO, DEPARA_MOVIMENTACAO]

    DEPARA_COMBO_bronze >> DEPARA_COMBO_silver
    DEPARA_NORMAS_bronze >> DEPARA_NORMAS_silver

    tasks >> demanda_bronze >> demanda_silver
    tasks >> carteira_bronze >> carteira_silver
    tasks >> SKU_CLASSIFICACAO_bronze >> SKU_CLASSIFICACAO_silver
    tasks >> MB51_bronze >> MB51_silver
    tasks >> ZSDI19_bronze >> ZSDI19_silver
    tasks >> TRANSACAO_ESTOQUE_bronze >> TRANSACAO_ESTOQUE_silver
    tasks >> ZV36_bronze >> ZV36_silver
    tasks >> FATURA_bronze >> FATURA_silver
    tasks >> ESTOQUE_bronze >> ESTOQUE_silver
    [demanda_silver, carteira_silver, SKU_CLASSIFICACAO_silver, MB51_silver, ZSDI19_silver, TRANSACAO_ESTOQUE_silver,
     ZV36_silver, FATURA_silver, ESTOQUE_silver] \
        >> gold_operations_demanda_inicial >> gold_operations_estoque >> gold_operations_faturamento_serie_temporal >> gold_operations_predicao
