import dlt
from pyspark.sql.functions import *

from utilities.Rules import get_rules_as_list_of_dict
_RULES_CACHE = get_rules_as_list_of_dict()

#Activate this function for tag specific scenario
def get_rule(tag):
    return {
        row["name"] : row["constraint"]
        for row in _RULES_CACHE
        if row["tag"] == tag
    }

#Grab the environment for dynamic reading
env_type = spark.conf.get("pipeline.env", "dev").lower()

#Activate this function for ALL rules
def get_rules():
    return {
        row["name"] : row["constraint"]
        for row in _RULES_CACHE
    }

# def to_minutes(col_name):
#     return (floor(col(col_name) / 100) * 60) + (col(col_name) % 100)

@dlt.view(
    name = "transformations",
    comment = "This is a view for transformations to be used in Gold Layer"
)
@dlt.expect_all_or_drop(get_rules())
def transformations():
    # Read data from Bronze
    df = dlt.read_stream(f"Flight_Delay_Bronze_{env_type}")

    #Rename Columns--
    df = df.withColumnRenamed("FL_DATE","FLIGHT_DATE")
    df = df.withColumnRenamed("OP_CARRIER","AIRLINE_CODE")
    df = df.withColumnRenamed("OP_CARRIER_FL_NUM","FLIGHT_NUMBER")
    df = df.withColumnRenamed("ORIGIN","ORIGIN_AIRPORT")
    df = df.withColumnRenamed("DEST","DEST_AIRPORT")
    df = df.withColumnRenamed("CRS_DEP_TIME","SCHEDULED_DEP_TIME")
    df = df.withColumnRenamed("DEP_TIME","ACTUAL_DEP_TIME")
    df = df.withColumnRenamed("DEP_DELAY","DEP_DELAY_MINUTES")
    df = df.withColumnRenamed("CRS_ARR_TIME","SCHEDULED_ARR_TIME")
    df = df.withColumnRenamed("ARR_TIME","ACTUAL_ARR_TIME")
    df = df.withColumnRenamed("ARR_DELAY","ARR_DELAY_MINUTES")
    df = df.withColumnRenamed("CANCELLED","IS_CANCELLED")
    df = df.withColumnRenamed("DIVERTED","IS_DIVERTED")
    df = df.withColumnRenamed("AIR_TIME","AIR_TIME_MINUTES")
    df = df.withColumnRenamed("DISTANCE","DISTANCE_MILES")
    df = df.withColumnRenamed("CARRIER_DELAY","DELAY_CARRIER")
    df = df.withColumnRenamed("WEATHER_DELAY","DELAY_WEATHER")
    df = df.withColumnRenamed("NAS_DELAY","DELAY_NATIONAL_AIR_SYSTEM")
    df = df.withColumnRenamed("SECURITY_DELAY","DELAY_SECURITY")
    df = df.withColumnRenamed("LATE_AIRCRAFT_DELAY","DELAY_AIR_SYSTEM")
    df = df.withColumnRenamed("CRS_ELAPSED_TIME","ESTIMATED_ELAPSED_TIME")


    # This is for filling nulls with 0
    df = df.fillna(
        value=0,
        subset=[
            "IS_DIVERTED",
            "DELAY_AIR_SYSTEM",
            "DELAY_SECURITY",
            "DELAY_NATIONAL_AIR_SYSTEM",
            "DELAY_WEATHER",
            "DELAY_CARRIER",
            "ARR_DELAY_MINUTES",
            "DEP_DELAY_MINUTES",
        ],
    )

    # This refactors different time columns to 0 for cancelled flights
    cols_to_fix = ["AIR_TIME_MINUTES", "ACTUAL_ELAPSED_TIME", "ESTIMATED_ELAPSED_TIME"]
    for c in cols_to_fix:
        df = df.withColumn(c, when(col("IS_CANCELLED") == 1, 0).otherwise(col(c)))

    # This refactors Cancellation code to "U" for cancelled flights
    df = df.withColumn(
        "CANCELLATION_CODE",
        when(
            col("IS_CANCELLED") == 1, coalesce(col("CANCELLATION_CODE"), lit("U"))
        ).otherwise(lit("N/A"))
    )

    # This keeps only "Valid" physical flights
    df = df.filter((col("DISTANCE_MILES") > 0) & (col("DISTANCE_MILES").isNotNull()))

    # Delay calculation
    df = df.withColumn(
        "TOTAL_DELAY_CHECKED",
        0
        + col("DELAY_CARRIER")
        + col("DELAY_WEATHER")
        + col("DELAY_NATIONAL_AIR_SYSTEM")
        + col("DELAY_SECURITY")
        + col("DELAY_AIR_SYSTEM")
    )

    #Consistency Check
    df = df.withColumn(
    "IS_DELAY_DATA_CONSISTENT",
    col("TOTAL_DELAY_CHECKED") == col("ARR_DELAY_MINUTES")
    )

    #Route_ID for faster Read in BI data
    df = df.withColumn(
        "ROUTE_ID", concat_ws("-", col("ORIGIN_AIRPORT"), col("DEST_AIRPORT"))
    )

    #Flight Identity
    df = df.withColumn("FLIGHT_ID", concat(col("AIRLINE_CODE"), lit("-"), col("FLIGHT_NUMBER")))

    #Temporal Features
    df = df.withColumn("FLIGHT_MONTH", month(col("FLIGHT_DATE")))
    df = df.withColumn("FLIGHT_QUARTER", quarter(col("FLIGHT_DATE")))
    df = df.withColumn("DAY_OF_WEEK", date_format(col("FLIGHT_DATE"), "EEEE"))

    #Efficiency Metric (MPH)
    df = df.withColumn(
        "AVG_GROUND_SPEED_MPH",
        when(col("AIR_TIME_MINUTES") > 0, (col("DISTANCE_MILES") / col("AIR_TIME_MINUTES")) * 60)
        .otherwise(0)
    )

    #Distance categories (for route analysis)
    df = df.withColumn("FLIGHT_DISTANCE_CATEGORY",
    when(col("DISTANCE_MILES") < 500, "Short-haul")
    .when(col("DISTANCE_MILES") < 1500, "Medium-haul")
    .otherwise("Long-haul")
    )   

    #Delay Categorization
    df = df.withColumn("DELAY_SEVERITY", 
        when(col("ARR_DELAY_MINUTES") <= 0, "On-time")
        .when(col("ARR_DELAY_MINUTES") <= 15, "Minor")
        .when(col("ARR_DELAY_MINUTES") <= 60, "Moderate")
        .otherwise("Major")
    )

    #Audit Metadata
    df = df.withColumn("_ingested_at", current_timestamp())

    #Hash Column: Used md5 because it is not security sensitive data
    df = df.withColumn(
    "KEY_COLUMN",
        md5(
            concat_ws(
                "|",
                col("AIRLINE_CODE"),
                col("FLIGHT_NUMBER"),
                col("FLIGHT_DATE"),
                col("ORIGIN_AIRPORT"),
                col("DEST_AIRPORT"),
                col("SCHEDULED_DEP_TIME")  # Handles same flight # multiple times/day
            )
        )
    )


    return df


@dlt.table(
    name = f"Flight_Delay_Silver_{env_type}",
    comment = "Silver table for raw airline data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.logRetentionDuration": "interval 30 days", 
        "quality": "silver",
        "project": "airline_delay_analysis"
    }
)
def Flight_Delay_Silver():
    return dlt.read_stream("transformations")

