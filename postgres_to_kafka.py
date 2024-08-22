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

# Configura Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Leitura do PostgreSQL usando Spark
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .load()

# Converte os dados para JSON
df_json = df.select(to_json(struct([df[x] for x in df.columns])).alias("value"))

# Função para enviar os dados para o Kafka
def send_to_kafka(batch_df, batch_id):
    for row in batch_df.collect():
        producer.send('covid_data', value=json.loads(row.value))
    producer.flush()

# Configura o Spark Streaming para processar os dados em micro-batches
df_json.writeStream \
    .foreachBatch(send_to_kafka) \
    .outputMode("update") \
    .start() \
    .awaitTermination()