from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

# Cria a Spark Session
spark = SparkSession.builder \
        .appName("CovidDataProcessing") \
        .config("spark.jars", "/opt/bitnami/spark/kafka/debezium-connector-postgres/postgresql-42.6.1.jar") \
        .config("spark.jars", "postgresql-42.6.1.jar") \
        .config('spark.driver.extraClassPath', '/opt/bitnami/spark/kafka/debezium-connector-postgres/postgresql-42.6.1.jar') \
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
    .option("sep", ";") \
    .option("header", "true") \
    .schema(schema) \
    .csv('./data')

# Configura a conexão com o PostgreSQL
postgres_url = "jdbc:postgresql://postgres:5432/airflow_db"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Função para processar cada microbatch e escrever no PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .mode('append') \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("driver", properties["driver"]) \
        .option("dbtable", 'covid_data') \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .save()

# Escreve os dados em tempo real no PostgreSQL usando foreachBatch
df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start() \
    .awaitTermination()
