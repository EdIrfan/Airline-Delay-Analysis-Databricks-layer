import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import *

# --- 1. PARAMETER RETRIEVAL (The Handshake) ---
# We fetch the values sent by the Lambda trigger.
env_type = spark.conf.get("pipeline.env", "dev")
landing_bucket_path = spark.conf.get("pipeline.landing_path")

# --- 2. DEFINE THE SOURCE SCHEMA ---
# Keeping this strictly defined to ensure data quality at the gate
schema_var = StructType([
    StructField("FL_DATE", DateType(), True),
    StructField("OP_CARRIER", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", IntegerType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("DEST", StringType(), True),
    StructField("CRS_DEP_TIME", IntegerType(), True),
    StructField("DEP_TIME", IntegerType(), True),
    StructField("DEP_DELAY", IntegerType(), True),
    StructField("TAXI_OUT", IntegerType(), True),
    StructField("WHEELS_OFF", IntegerType(), True),
    StructField("WHEELS_ON", IntegerType(), True),
    StructField("TAXI_IN", IntegerType(), True),
    StructField("CRS_ARR_TIME", IntegerType(), True),
    StructField("ARR_TIME", IntegerType(), True),
    StructField("ARR_DELAY", IntegerType(), True),
    StructField("CANCELLED", IntegerType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", IntegerType(), True),
    StructField("CRS_ELAPSED_TIME", IntegerType(), True),
    StructField("ACTUAL_ELAPSED_TIME", IntegerType(), True),
    StructField("AIR_TIME", IntegerType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("CARRIER_DELAY", IntegerType(), True),
    StructField("WEATHER_DELAY", IntegerType(), True),
    StructField("NAS_DELAY", IntegerType(), True),
    StructField("SECURITY_DELAY", IntegerType(), True),
    StructField("LATE_AIRCRAFT_DELAY", IntegerType(), True),
    StructField("Unnamed: 27", StringType(), True)
])

# --- 3. BRONZE TABLE DEFINITION with faulty column removal---
@dlt.table(
    # Dynamic naming: Flight_Delay_Bronze_dev or Flight_Delay_Bronze_prod
    name=f"Flight_Delay_Bronze_{env_type.lower()}",
    comment="Bronze layer for raw airline ingestion. Parameterized by AWS Lambda.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "env": env_type
    }
)
def flight_delay_bronze_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema_var)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(landing_bucket_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_ID", col("_metadata.file_path"))
        .drop("Unnamed: 27")
    )