import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'etl_pipeline',
    description='ETL pipeline with Kafka, Spark, and PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024,8,23),
    catchup=False,
)

datalake_to_kafka = SparkSubmitOperator(
    task_id='t1',
    application='/opt/bitnami/spark/datalake_to_kafka.py',
    conn_id='spark-master',
    packages= "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    name='datalake_to_kafka',
    dag=dag,
)

kafka_to_postgres = SparkSubmitOperator(
    task_id='t2',
    application='/opt/bitnami/spark/kafka_to_postgres.py',
    conn_id='spark-master',
    packages= "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.1",
    name='kafka_to_postgres',
    dag=dag,
)

postgres_to_kafka = SparkSubmitOperator(
    task_id='t3',
    application='/opt/bitnami/spark/postgres_to_kafka.py',
    conn_id='spark-master',
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.1",
    name='postgres_to_kafka',
    dag=dag,
)

kafka_to_console = SparkSubmitOperator(
    task_id='t4',
    application='/opt/bitnami/spark/kafka_to_console.py',
    conn_id='spark-master',
    packages = "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    name='kafka_to_console',
    dag=dag,
)

datalake_to_kafka >> kafka_to_postgres >> postgres_to_kafka >> kafka_to_console