from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_csv
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

# Cria a Spark Session
spark = SparkSession.builder \
        .appName("KafkatoPostgres") \
        .getOrCreate()

# Configurações para o Kafka e Schema Registry
kafka_bootstrap_servers = "kafka:29092"
topic_name = "covid_data"

# Schema dos dados JSON
json_schema = StructType([
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

# Consome dados do Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Converte a mensagem do Kafka para String e aplica o schema JSON
df_kafka = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")

df = df_kafka.select("iso_code", "continent","location","date","total_cases","total_deaths","reproduction_rate","icu_patients","hosp_patients","weekly_icu_admissions","weekly_hosp_admissions","total_tests","total_vaccinations","people_vaccinated","population","population_density","median_age","aged_65_older","aged_70_older","gdp_per_capita","extreme_poverty","excess_mortality")

# Função para salvar os dados no PostgreSQL
def save_to_postgres(batch_df, batch_id):
    batch_df.show(5, truncate=False)
    
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow_db") \
        .option("dbtable", "covid_cases") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("append") \
        .save()
        
# Escreve o stream de dados no PostgreSQL
query = df.writeStream \
    .foreachBatch(save_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()