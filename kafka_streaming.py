from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_csv
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

def process_kafka_stream():
    # Criação da SparkSession com suporte ao Kafka
    spark = SparkSession.builder \
        .appName("FileStreamProcessing") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "/kafka/debezium-connector-postgres/postgresql-42.6.1.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

    # Leitura do stream do Kafka
    kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "datalake") \
            .option("startingOffsets", "earliest") \
            .load()

    # Decodificar os dados do Kafka (os valores são em binário)
    kafka_stream_df = kafka_stream.selectExpr("CAST(value AS STRING) as message")

    # Transformação de dados (exemplo: convertendo JSON string em colunas)
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

    processed_stream = kafka_stream_df \
            .select(from_csv(col("message"), schema).alias("data")) \
            .select("data.*")

    # Escrita dos dados processados em outro local ou console
    query = processed_stream \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_kafka_stream()