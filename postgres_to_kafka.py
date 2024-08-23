from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
import json

# Cria a Spark Session
spark = SparkSession.builder \
    .appName("Postgres-to-Kafka-Spark-Streaming") \
    .config("spark.jars", "/kafka/debezium-connector-postgres/postgresql-42.6.1.jar") \
    .config("spark.jars", "spark-streaming_2.12-3.5.0.jar") \
    .config("spark.jars", "spark-sql_2.12-3.5.0.jar") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/bitnami/spark/checkpoints") \
    .getOrCreate()
    

# Configurações do PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
table_name = "covid_new_rate"
db_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Leitura dos dados da tabela 'covid_new_rate' no Postgres
df_postgres = spark.read.jdbc(url=jdbc_url, table="covid_new_rate", properties=db_properties)

# Converter dados para JSON
df_json = df_postgres.select(to_json(struct("*")).alias("value"))

# Escreve o stream de dados para o Kafka
df_json.write \
 .format("kafka") \
 .option("kafka.bootstrap.servers", "kafka:29092") \
 .option("topic", "covid_new_rate") \
 .save()