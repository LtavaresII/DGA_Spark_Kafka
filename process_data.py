from pyspark.sql import SparkSession

def spark_connection():
    spark = SparkSession.builder \
        .appName("CovidDataProcessing") \
        .config("spark.jars", "/kafka/debezium-connector-postgres/postgresql-42.6.1.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1","org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
        .getOrCreate()
    
    return spark

def kafka_connection(spark):
    kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "datalake") \
        .option("startingOffsets", "earliest") \
        .load()
    return kafka

def read_data_csv(csv,spark):
    csv_df = spark \
        .readStream \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(csv)
    return csv_df

def read_data_json(json,spark):
    json_df = spark \
        .readStream \
        .format("json") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(json)
    return json_df

def get_cases(data_df):
    cases_df = data_df.select("iso_code","continent","location", "date","total_cases","median_age","aged_65_older","aged_70_older","gdp_per_capita","extreme_poverty")
    cases_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/covid_db") \
        .option("dbtable", "covid_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()
    return cases_df

def get_deaths(data_df):
    deaths_df = data_df.select("iso_code","continent","location", "date", "total_deaths","median_age","aged_65_older","aged_70_older","gdp_per_capita","extreme_poverty")
    deaths_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/covid_db") \
        .option("dbtable", "covid_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()
    return deaths_df

def get_hospitalizations(data_df):
    hospitalizations_df = data_df.select("iso_code","continent","location","date","hosp_patients","weekly_hosp_admissions","median_age","aged_65_older","aged_70_older")
    hospitalizations_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/covid_db") \
        .option("dbtable", "covid_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()
    return hospitalizations_df

def get_vaccinations(data_df):
    vaccinations_df = data_df.select("iso_code","location", "date", "total_vaccinations","daily_people_vaccinated","median_age","aged_65_older","aged_70_older")
    vaccinations_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/covid_db") \
        .option("dbtable", "covid_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()
    return vaccinations_df

if __name__ == "__main__":
    spark = spark_connection()

    if spark is not None:
        # connect to kafka with spark connection
        kafka = kafka_connection(spark)