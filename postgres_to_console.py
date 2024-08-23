from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from kafka import KafkaProducer
import json

# Cria a Spark Session
spark = SparkSession.builder \
    .appName("Postgres-to-Kafka-Spark-Streaming") \
    .config("spark.jars", "/kafka/debezium-connector-postgres/postgresql-42.6.1.jar") \
    .config("spark.jars", "spark-streaming_2.12-3.5.0.jar") \
    .config("spark.jars", "spark-sql_2.12-3.5.0.jar") \
    .getOrCreate()
    

# Configurações do PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"
table_name = "covid_data"
db_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Leitura dos dados da tabela 'processed_table' no Postgres
df_postgres = spark.read.jdbc(url=jdbc_url, table="covid_data", properties=db_properties)

# Configuração do stream
query = df_postgres.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Manter o stream em execução
query.awaitTermination()