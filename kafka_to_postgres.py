from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_csv
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

# Cria a Spark Session
spark = SparkSession.builder \
        .appName("KafkatoConsole") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')\
        .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

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
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "covid") \
    .option("startingOffsets", "earliest") \
    .load()

# Converte a mensagem do Kafka para String e aplica o schema JSON
df_kafka = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")

# Pega os dados apenas a partir de 01-03-2020
df = df_kafka.filter(col("date") >= "2020-03-01")

# Criação da taxa de mortalidade e taxa de vacinação
df = df.withColumn("mortality_rate", (col("total_deaths") / col("total_cases")) * 100)
df = df.withColumn("vaccination_rate", (col("people_vaccinated") / (col("population") - col("total_deaths"))) * 100)

# Calcular o número de pessoas para cada porcentagem
df = df.withColumn("people_median_age", (col("median_age") / 100) * (col("population") - col("total_deaths")))
df = df.withColumn("people_aged_65_older", (col("aged_65_older") / 100) * (col("population") - col("total_deaths")))
df = df.withColumn("people_aged_70_older", (col("aged_70_older") / 100) * (col("population") - col("total_deaths")))

# Calcular a porcentagem de mortes em relação a cada grupo de idade (meia idade, acima de 65 e acima de 70)
df = df.withColumn("death_percentage_median_age", (col("total_deaths") / col("people_median_age")) * 100)
df = df.withColumn("death_percentage_aged_65_older", (col("total_deaths") / col("people_aged_65_older")) * 100)
df = df.withColumn("death_percentage_aged_70_older", (col("total_deaths") / col("people_aged_70_older")) * 100)

# Redução/Seleção de dados
df = df.select("location","date","total_cases","new_cases","total_deaths","new_deaths","mortality_rate","death_percentage_median_age","death_percentage_aged_65_older","death_percentage_aged_70_older","total_vaccinations","people_vaccinated","new_vaccinations","vaccination_rate","population")

# Definição da Função para Gravar os Dados no PostgreSQL
def save_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "covid_new_rate") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .save()

#Aplicar a Função save_to_postgres a Cada Micro-batch
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(save_to_postgres) \
    .start()

#Esperar a Terminação do Stream
query.awaitTermination(10)
