from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Cria uma SparkSession
spark = SparkSession.builder \
    .appName("Kafka-Spark-Streaming") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')\
    .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

# Consome dados do Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "covid_new_rate") \
    .option("startingOffsets", "earliest") \
    .load()

# Converte os valores das mensagens de binário para string
df_kafka = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Exibe os dados no console
query = df_kafka.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Manter o stream em execução
query.awaitTermination(10)