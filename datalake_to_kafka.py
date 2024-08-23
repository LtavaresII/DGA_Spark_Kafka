from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
from pyspark.sql.functions import to_json, struct
#from kafka import KafkaProducer
import json

# Cria a Spark Session
spark = SparkSession.builder \
    .appName("DatalaketoKafka") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .config("spark.sql.streaming.checkpointLocation", "/opt/bitnami/spark/checkpoints") \
    .getOrCreate()

schema = StructType([
        StructField("iso_code", StringType(), True),
        StructField("continent", StringType(), True),
        StructField("location", StringType(), True),
        StructField("date", DateType(), True),
        StructField("total_cases", DoubleType(), True),
        StructField("new_cases", DoubleType(), True),
        StructField("total_deaths", DoubleType(), True),
        StructField("new_deaths", DoubleType(), True),
        StructField("total_cases_per_million", DoubleType(), True),
        StructField("new_cases_per_million", DoubleType(), True),
        StructField("total_deaths_per_million", DoubleType(), True),
        StructField("new_deaths_per_million", DoubleType(), True),
        StructField("reproduction_rate", DoubleType(), True),
        StructField("icu_patients", DoubleType(), True),
        StructField("icu_patients_per_million", DoubleType(), True),
        StructField("hosp_patients", DoubleType(), True),
        StructField("hosp_patients_per_million", DoubleType(), True),
        StructField("weekly_icu_admissions", DoubleType(), True),
        StructField("weekly_icu_admissions_per_million", DoubleType(), True),
        StructField("weekly_hosp_admissions", DoubleType(), True),
        StructField("weekly_hosp_admissions_per_million", DoubleType(), True),
        StructField("new_tests", DoubleType(), True),
        StructField("total_tests", DoubleType(), True),
        StructField("total_tests_per_thousand", DoubleType(), True),
        StructField("new_tests_per_thousand", DoubleType(), True),
        StructField("positive_rate", DoubleType(), True),
        StructField("tests_per_case", DoubleType(), True),
        StructField("tests_units", StringType(), True),
        StructField("total_vaccinations", DoubleType(), True),
        StructField("people_vaccinated", DoubleType(), True),
        StructField("people_fully_vaccinated", DoubleType(), True),
        StructField("total_boosters", DoubleType(), True),
        StructField("new_vaccinations", DoubleType(), True),
        StructField("new_vaccinations_smoothed", DoubleType(), True),
        StructField("total_vaccinations_per_hundred", DoubleType(), True),
        StructField("people_vaccinated_per_hundred", DoubleType(), True),
        StructField("people_fully_vaccinated_per_hundred", DoubleType(), True),
        StructField("total_boosters_per_hundred", DoubleType(), True),
        StructField("new_vaccinations_smoothed_per_million", DoubleType(), True),
        StructField("stringency_index", DoubleType(), True),
        StructField("population", DoubleType(), True),
        StructField("population_density", DoubleType(), True),
        StructField("median_age", DoubleType(), True),
        StructField("aged_65_older", DoubleType(), True),
        StructField("aged_70_older", DoubleType(), True),
        StructField("gdp_per_capita", DoubleType(), True),
        StructField("extreme_poverty", DoubleType(), True),
        StructField("cardiovasc_death_rate", DoubleType(), True),
        StructField("diabetes_prevalence", DoubleType(), True),
        StructField("female_smokers", DoubleType(), True),
        StructField("male_smokers", DoubleType(), True),
        StructField("handwashing_facilities", DoubleType(), True),
        StructField("hospital_beds_per_thousand", DoubleType(), True),
        StructField("life_expectancy", DoubleType(), True),
        StructField("human_development_index", DoubleType(), True),
        StructField("excess_mortality", DoubleType(), True)
])

# Leitura dos dados do CSV como um stream de dados
df = spark.readStream \
    .format("csv") \
    .option("sep", ",") \
    .option("header", "true") \
    .schema(schema) \
    .option("path", '/opt/bitnami/spark/data') \
    .option("maxFilesPerTrigger", 1) \
    .load()

# Converte os dados para JSON
df_json = df.select(to_json(struct([col(c) for c in df.columns])).alias("value"))

# Escrever o stream no Kafka
query = df_json.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "covid") \
    .start()

# Aguardar até que o streaming termine
query.awaitTermination()