import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Grab environment for dynamic naming
env_type = spark.conf.get("pipeline.env", "dev").lower()

# ============================================================================
# GOLD LAYER - AGGREGATE TABLES FOR ANALYTICS
# ============================================================================

# 1. DAILY AIRLINE PERFORMANCE - KPIs by carrier and date
@dlt.table(
    name=f"Gold_Daily_Airline_Performance_{env_type}",
    comment="Daily aggregated metrics by airline for performance dashboards",
    table_properties={
        "quality": "gold",
        "project": "airline_delay_analysis",
        "pipelines.autoOptimize.zOrderCols": "FLIGHT_DATE,AIRLINE_CODE"
    }
)
def Gold_Daily_Airline_Performance():
    df = dlt.read_stream("transformations")
    
    return df.groupBy("FLIGHT_DATE", "AIRLINE_CODE") \
        .agg(
            count("*").alias("TOTAL_FLIGHTS"),
            sum(when(col("IS_CANCELLED") == 1, 1).otherwise(0)).alias("CANCELLED_FLIGHTS"),
            sum(when(col("IS_DIVERTED") == 1, 1).otherwise(0)).alias("DIVERTED_FLIGHTS"),
            sum(when(col("DELAY_SEVERITY") == "On-time", 1).otherwise(0)).alias("ON_TIME_FLIGHTS"),
            sum(when(col("ARR_DELAY_MINUTES") > 15, 1).otherwise(0)).alias("DELAYED_FLIGHTS"),
            
            # Average delays
            avg("DEP_DELAY_MINUTES").alias("AVG_DEP_DELAY"),
            avg("ARR_DELAY_MINUTES").alias("AVG_ARR_DELAY"),
            
            # Delay breakdown
            sum("DELAY_CARRIER").alias("TOTAL_CARRIER_DELAY"),
            sum("DELAY_WEATHER").alias("TOTAL_WEATHER_DELAY"),
            sum("DELAY_NATIONAL_AIR_SYSTEM").alias("TOTAL_NAS_DELAY"),
            sum("DELAY_SECURITY").alias("TOTAL_SECURITY_DELAY"),
            sum("DELAY_AIR_SYSTEM").alias("TOTAL_AIRCRAFT_DELAY"),
            
            # Operational metrics
            avg("AIR_TIME_MINUTES").alias("AVG_AIR_TIME"),
            avg("DISTANCE_MILES").alias("AVG_DISTANCE"),
            avg("AVG_GROUND_SPEED_MPH").alias("AVG_SPEED_MPH"),
            
            # On-time performance percentage
            (sum(when(col("ARR_DELAY_MINUTES") <= 15, 1).otherwise(0)) / count("*") * 100).alias("ON_TIME_PERCENTAGE")
        ) \
        .withColumn("_updated_at", current_timestamp())


# 2. ROUTE PERFORMANCE - Metrics by origin-destination pairs
@dlt.table(
    name=f"Gold_Route_Performance_{env_type}",
    comment="Aggregated performance metrics by route for route analysis",
    table_properties={
        "quality": "gold",
        "project": "airline_delay_analysis",
        "pipelines.autoOptimize.zOrderCols": "ORIGIN_AIRPORT,DEST_AIRPORT"
    }
)
def Gold_Route_Performance():
    df = dlt.read_stream("transformations")
    
    return df.groupBy("ROUTE_ID", "ORIGIN_AIRPORT", "DEST_AIRPORT", "FLIGHT_DISTANCE_CATEGORY") \
        .agg(
            count("*").alias("TOTAL_FLIGHTS"),
            countDistinct("AIRLINE_CODE").alias("NUM_CARRIERS"),
            
            # Delay metrics
            avg("ARR_DELAY_MINUTES").alias("AVG_ARRIVAL_DELAY"),
            percentile_approx("ARR_DELAY_MINUTES", 0.5).alias("MEDIAN_ARRIVAL_DELAY"),
            max("ARR_DELAY_MINUTES").alias("MAX_ARRIVAL_DELAY"),
            
            # Reliability metrics
            (sum(when(col("IS_CANCELLED") == 1, 1).otherwise(0)) / count("*") * 100).alias("CANCELLATION_RATE"),
            (sum(when(col("ARR_DELAY_MINUTES") <= 15, 1).otherwise(0)) / count("*") * 100).alias("ON_TIME_RATE"),
            
            # Distance and time
            avg("DISTANCE_MILES").alias("AVG_DISTANCE"),
            avg("AIR_TIME_MINUTES").alias("AVG_AIR_TIME"),
            avg("AVG_GROUND_SPEED_MPH").alias("AVG_SPEED"),
            
            # Most common delay cause
            sum("DELAY_CARRIER").alias("TOTAL_CARRIER_DELAY"),
            sum("DELAY_WEATHER").alias("TOTAL_WEATHER_DELAY"),
            sum("DELAY_NATIONAL_AIR_SYSTEM").alias("TOTAL_NAS_DELAY")
        ) \
        .withColumn("_updated_at", current_timestamp())


# 3. AIRPORT STATISTICS - Performance by airport (origin & destination separately)
@dlt.table(
    name=f"Gold_Airport_Statistics_{env_type}",
    comment="Airport-level operational metrics for origin and destination analysis",
    table_properties={
        "quality": "gold",
        "project": "airline_delay_analysis"
    }
)
def Gold_Airport_Statistics():
    df = dlt.read_stream("transformations")
    
    # Origin airport stats
    origin_stats = df.groupBy("ORIGIN_AIRPORT") \
        .agg(
            count("*").alias("DEPARTURES"),
            avg("DEP_DELAY_MINUTES").alias("AVG_DEP_DELAY"),
            (sum(when(col("IS_CANCELLED") == 1, 1).otherwise(0)) / count("*") * 100).alias("CANCELLATION_RATE_DEP")
        ) \
        .withColumnRenamed("ORIGIN_AIRPORT", "AIRPORT_CODE")
    
    # Destination airport stats
    dest_stats = df.groupBy("DEST_AIRPORT") \
        .agg(
            count("*").alias("ARRIVALS"),
            avg("ARR_DELAY_MINUTES").alias("AVG_ARR_DELAY"),
            (sum(when(col("ARR_DELAY_MINUTES") <= 15, 1).otherwise(0)) / count("*") * 100).alias("ON_TIME_ARRIVAL_RATE")
        ) \
        .withColumnRenamed("DEST_AIRPORT", "AIRPORT_CODE")
    
    # Combine both
    return origin_stats.join(dest_stats, "AIRPORT_CODE", "outer") \
        .fillna(0) \
        .withColumn("TOTAL_OPERATIONS", col("DEPARTURES") + col("ARRIVALS")) \
        .withColumn("_updated_at", current_timestamp())


# 4. MONTHLY TRENDS - Time-series aggregation for trend analysis
@dlt.table(
    name=f"Gold_Monthly_Trends_{env_type}",
    comment="Monthly performance trends across the entire airline industry",
    table_properties={
        "quality": "gold",
        "project": "airline_delay_analysis"
    }
)
def Gold_Monthly_Trends():
    df = dlt.read_stream("transformations")
    
    return df.groupBy(
        year("FLIGHT_DATE").alias("YEAR"),
        "FLIGHT_MONTH",
        "FLIGHT_QUARTER"
    ) \
        .agg(
            count("*").alias("TOTAL_FLIGHTS"),
            sum(when(col("IS_CANCELLED") == 1, 1).otherwise(0)).alias("CANCELLED_FLIGHTS"),
            avg("ARR_DELAY_MINUTES").alias("AVG_DELAY"),
            (sum(when(col("ARR_DELAY_MINUTES") <= 15, 1).otherwise(0)) / count("*") * 100).alias("ON_TIME_PERCENTAGE"),
            
            # Delay causes
            avg("DELAY_CARRIER").alias("AVG_CARRIER_DELAY"),
            avg("DELAY_WEATHER").alias("AVG_WEATHER_DELAY"),
            avg("DELAY_NATIONAL_AIR_SYSTEM").alias("AVG_NAS_DELAY"),
            
            # Operational metrics
            avg("DISTANCE_MILES").alias("AVG_DISTANCE"),
            avg("AVG_GROUND_SPEED_MPH").alias("AVG_SPEED")
        ) \
        .orderBy("YEAR", "FLIGHT_MONTH") \
        .withColumn("_updated_at", current_timestamp())


# 5. DELAY CAUSE ANALYSIS - Deep dive into delay categories
@dlt.table(
    name=f"Gold_Delay_Analysis_{env_type}",
    comment="Detailed breakdown of delay causes by airline and severity",
    table_properties={
        "quality": "gold",
        "project": "airline_delay_analysis"
    }
)
def Gold_Delay_Analysis():
    df = dlt.read_stream("transformations")
    
    # Filter only delayed flights
    delayed_df = df.filter(col("ARR_DELAY_MINUTES") > 15)
    
    return delayed_df.groupBy("AIRLINE_CODE", "DELAY_SEVERITY") \
        .agg(
            count("*").alias("DELAYED_FLIGHTS"),
            
            # Average delay by cause
            avg("DELAY_CARRIER").alias("AVG_CARRIER_DELAY"),
            avg("DELAY_WEATHER").alias("AVG_WEATHER_DELAY"),
            avg("DELAY_NATIONAL_AIR_SYSTEM").alias("AVG_NAS_DELAY"),
            avg("DELAY_SECURITY").alias("AVG_SECURITY_DELAY"),
            avg("DELAY_AIR_SYSTEM").alias("AVG_AIRCRAFT_DELAY"),
            
            # Total minutes lost
            sum("ARR_DELAY_MINUTES").alias("TOTAL_DELAY_MINUTES"),
            
            # Percentage of delays by cause
            (sum("DELAY_CARRIER") / sum("ARR_DELAY_MINUTES") * 100).alias("CARRIER_DELAY_PCT"),
            (sum("DELAY_WEATHER") / sum("ARR_DELAY_MINUTES") * 100).alias("WEATHER_DELAY_PCT"),
            (sum("DELAY_NATIONAL_AIR_SYSTEM") / sum("ARR_DELAY_MINUTES") * 100).alias("NAS_DELAY_PCT")
        ) \
        .withColumn("_updated_at", current_timestamp())


# 6. CANCELLATION ANALYSIS - Focus on cancelled flights
@dlt.table(
    name=f"Gold_Cancellation_Analysis_{env_type}",
    comment="Analysis of flight cancellations by reason and airline",
    table_properties={
        "quality": "gold",
        "project": "airline_delay_analysis"
    }
)
def Gold_Cancellation_Analysis():
    df = dlt.read_stream("transformations")
    
    # Filter only cancelled flights
    cancelled_df = df.filter(col("IS_CANCELLED") == 1)
    
    return cancelled_df.groupBy("AIRLINE_CODE", "CANCELLATION_CODE", "FLIGHT_MONTH") \
        .agg(
            count("*").alias("CANCELLED_FLIGHTS"),
            countDistinct("ROUTE_ID").alias("AFFECTED_ROUTES"),
            avg("DISTANCE_MILES").alias("AVG_DISTANCE")
        ) \
        .withColumn("_updated_at", current_timestamp())


# 7. DAY OF WEEK PERFORMANCE - Pattern analysis by day
@dlt.table(
    name=f"Gold_Day_Of_Week_Performance_{env_type}",
    comment="Performance patterns by day of week for scheduling optimization",
    table_properties={
        "quality": "gold",
        "project": "airline_delay_analysis"
    }
)
def Gold_Day_Of_Week_Performance():
    df = dlt.read_stream("transformations")
    
    return df.groupBy("DAY_OF_WEEK", "FLIGHT_DISTANCE_CATEGORY") \
        .agg(
            count("*").alias("TOTAL_FLIGHTS"),
            avg("ARR_DELAY_MINUTES").alias("AVG_DELAY"),
            (sum(when(col("ARR_DELAY_MINUTES") <= 15, 1).otherwise(0)) / count("*") * 100).alias("ON_TIME_RATE"),
            (sum(when(col("IS_CANCELLED") == 1, 1).otherwise(0)) / count("*") * 100).alias("CANCELLATION_RATE")
        ) \
        .withColumn("_updated_at", current_timestamp())