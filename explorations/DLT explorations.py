# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

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
])

# COMMAND ----------

df_2009 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2009.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2010 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2010.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2011 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2011.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2012 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2012.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2013 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2013.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2014 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2014.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2015 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2015.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2016 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2016.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2017 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2017.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2018 = spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_var)\
    .load("/Volumes/dlt_first_proper/source/csvrawdata/2018.csv")\
    .drop("Unnamed: 27")

# COMMAND ----------

df_2009.select(col("FL_DATE").alias("2009")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2010.select(col("FL_DATE").alias("2010")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2011.select(col("FL_DATE").alias("2011")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2012.select(col("FL_DATE").alias("2012")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2013.select(col("FL_DATE").alias("2013")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2014.select(col("FL_DATE").alias("2014")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2015.select(col("FL_DATE").alias("2015")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2016.select(col("FL_DATE").alias("2016")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2017.select(col("FL_DATE").alias("2017")).distinct().orderBy("FL_DATE", ascending = False).display()
df_2018.select(col("FL_DATE").alias("2018")).distinct().orderBy("FL_DATE", ascending = False).display()

# COMMAND ----------

df_2009.select(col("OP_CARRIER").alias("2009")).distinct().display()
df_2010.select(col("OP_CARRIER").alias("2010")).distinct().display()
df_2011.select(col("OP_CARRIER").alias("2011")).distinct().display()
df_2012.select(col("OP_CARRIER").alias("2012")).distinct().display()
df_2013.select(col("OP_CARRIER").alias("2013")).distinct().display()
df_2014.select(col("OP_CARRIER").alias("2014")).distinct().display()
df_2015.select(col("OP_CARRIER").alias("2015")).distinct().display()
df_2016.select(col("OP_CARRIER").alias("2016")).distinct().display()
df_2017.select(col("OP_CARRIER").alias("2017")).distinct().display()
df_2018.select(col("OP_CARRIER").alias("2018")).distinct().display()

# COMMAND ----------

df_2009.select(col("OP_CARRIER_FL_NUM").alias("2009")).distinct().display()
df_2010.select(col("OP_CARRIER_FL_NUM").alias("2010")).distinct().display()
df_2011.select(col("OP_CARRIER_FL_NUM").alias("2011")).distinct().display()
df_2012.select(col("OP_CARRIER_FL_NUM").alias("2012")).distinct().display()
df_2013.select(col("OP_CARRIER_FL_NUM").alias("2013")).distinct().display()
df_2014.select(col("OP_CARRIER_FL_NUM").alias("2014")).distinct().display()
df_2015.select(col("OP_CARRIER_FL_NUM").alias("2015")).distinct().display()
df_2016.select(col("OP_CARRIER_FL_NUM").alias("2016")).distinct().display()
df_2017.select(col("OP_CARRIER_FL_NUM").alias("2017")).distinct().display()
df_2018.select(col("OP_CARRIER_FL_NUM").alias("2018")).distinct().display()

# COMMAND ----------

df_2009.select(col("ORIGIN").alias("2009")).distinct().display()
df_2010.select(col("ORIGIN").alias("2010")).distinct().display()
df_2011.select(col("ORIGIN").alias("2011")).distinct().display()
df_2012.select(col("ORIGIN").alias("2012")).distinct().display()
df_2013.select(col("ORIGIN").alias("2013")).distinct().display()
df_2014.select(col("ORIGIN").alias("2014")).distinct().display()
df_2015.select(col("ORIGIN").alias("2015")).distinct().display()
df_2016.select(col("ORIGIN").alias("2016")).distinct().display()
df_2017.select(col("ORIGIN").alias("2017")).distinct().display()
df_2018.select(col("ORIGIN").alias("2018")).distinct().display()

# COMMAND ----------

df_2009.select(col("DEST").alias("2009")).distinct().display()
df_2010.select(col("DEST").alias("2010")).distinct().display()
df_2011.select(col("DEST").alias("2011")).distinct().display()
df_2012.select(col("DEST").alias("2012")).distinct().display()
df_2013.select(col("DEST").alias("2013")).distinct().display()
df_2014.select(col("DEST").alias("2014")).distinct().display()
df_2015.select(col("DEST").alias("2015")).distinct().display()
df_2016.select(col("DEST").alias("2016")).distinct().display()
df_2017.select(col("DEST").alias("2017")).distinct().display()
df_2018.select(col("DEST").alias("2018")).distinct().display()

# COMMAND ----------

df_2009.select(col("CRS_DEP_TIME").alias("2009")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2010.select(col("CRS_DEP_TIME").alias("2010")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2011.select(col("CRS_DEP_TIME").alias("2011")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2012.select(col("CRS_DEP_TIME").alias("2012")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2013.select(col("CRS_DEP_TIME").alias("2013")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2014.select(col("CRS_DEP_TIME").alias("2014")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2015.select(col("CRS_DEP_TIME").alias("2015")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2016.select(col("CRS_DEP_TIME").alias("2016")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2017.select(col("CRS_DEP_TIME").alias("2017")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()
df_2018.select(col("CRS_DEP_TIME").alias("2018")).distinct().orderBy(col("CRS_DEP_TIME"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("DEP_TIME").alias("2009")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2010.select(col("DEP_TIME").alias("2010")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2011.select(col("DEP_TIME").alias("2011")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2012.select(col("DEP_TIME").alias("2012")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2013.select(col("DEP_TIME").alias("2013")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2014.select(col("DEP_TIME").alias("2014")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2015.select(col("DEP_TIME").alias("2015")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2016.select(col("DEP_TIME").alias("2016")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2017.select(col("DEP_TIME").alias("2017")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()
df_2018.select(col("DEP_TIME").alias("2018")).distinct().orderBy(col("DEP_TIME"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("DEP_DELAY").alias("2009")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2010.select(col("DEP_DELAY").alias("2010")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2011.select(col("DEP_DELAY").alias("2011")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2012.select(col("DEP_DELAY").alias("2012")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2013.select(col("DEP_DELAY").alias("2013")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2014.select(col("DEP_DELAY").alias("2014")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2015.select(col("DEP_DELAY").alias("2015")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2016.select(col("DEP_DELAY").alias("2016")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2017.select(col("DEP_DELAY").alias("2017")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()
df_2018.select(col("DEP_DELAY").alias("2018")).distinct().orderBy(col("DEP_DELAY"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("TAXI_OUT").alias("2009")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2010.select(col("TAXI_OUT").alias("2010")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2011.select(col("TAXI_OUT").alias("2011")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2012.select(col("TAXI_OUT").alias("2012")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2013.select(col("TAXI_OUT").alias("2013")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2014.select(col("TAXI_OUT").alias("2014")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2015.select(col("TAXI_OUT").alias("2015")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2016.select(col("TAXI_OUT").alias("2016")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2017.select(col("TAXI_OUT").alias("2017")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()
df_2018.select(col("TAXI_OUT").alias("2018")).distinct().orderBy(col("TAXI_OUT"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("WHEELS_OFF").alias("2009")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2010.select(col("WHEELS_OFF").alias("2010")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2011.select(col("WHEELS_OFF").alias("2011")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2012.select(col("WHEELS_OFF").alias("2012")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2013.select(col("WHEELS_OFF").alias("2013")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2014.select(col("WHEELS_OFF").alias("2014")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2015.select(col("WHEELS_OFF").alias("2015")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2016.select(col("WHEELS_OFF").alias("2016")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2017.select(col("WHEELS_OFF").alias("2017")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()
df_2018.select(col("WHEELS_OFF").alias("2018")).distinct().orderBy(col("WHEELS_OFF"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("TAXI_IN").alias("2009")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2010.select(col("TAXI_IN").alias("2010")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2011.select(col("TAXI_IN").alias("2011")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2012.select(col("TAXI_IN").alias("2012")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2013.select(col("TAXI_IN").alias("2013")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2014.select(col("TAXI_IN").alias("2014")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2015.select(col("TAXI_IN").alias("2015")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2016.select(col("TAXI_IN").alias("2016")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2017.select(col("TAXI_IN").alias("2017")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()
df_2018.select(col("TAXI_IN").alias("2018")).distinct().orderBy(col("TAXI_IN"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("CRS_ARR_TIME").alias("2009")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2010.select(col("CRS_ARR_TIME").alias("2010")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2011.select(col("CRS_ARR_TIME").alias("2011")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2012.select(col("CRS_ARR_TIME").alias("2012")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2013.select(col("CRS_ARR_TIME").alias("2013")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2014.select(col("CRS_ARR_TIME").alias("2014")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2015.select(col("CRS_ARR_TIME").alias("2015")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2016.select(col("CRS_ARR_TIME").alias("2016")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2017.select(col("CRS_ARR_TIME").alias("2017")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()
df_2018.select(col("CRS_ARR_TIME").alias("2018")).distinct().orderBy(col("CRS_ARR_TIME"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("ARR_TIME").alias("2009")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2010.select(col("ARR_TIME").alias("2010")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2011.select(col("ARR_TIME").alias("2011")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2012.select(col("ARR_TIME").alias("2012")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2013.select(col("ARR_TIME").alias("2013")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2014.select(col("ARR_TIME").alias("2014")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2015.select(col("ARR_TIME").alias("2015")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2016.select(col("ARR_TIME").alias("2016")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2017.select(col("ARR_TIME").alias("2017")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()
df_2018.select(col("ARR_TIME").alias("2018")).distinct().orderBy(col("ARR_TIME"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("ARR_DELAY").alias("2009")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2010.select(col("ARR_DELAY").alias("2010")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2011.select(col("ARR_DELAY").alias("2011")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2012.select(col("ARR_DELAY").alias("2012")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2013.select(col("ARR_DELAY").alias("2013")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2014.select(col("ARR_DELAY").alias("2014")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2015.select(col("ARR_DELAY").alias("2015")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2016.select(col("ARR_DELAY").alias("2016")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2017.select(col("ARR_DELAY").alias("2017")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()
df_2018.select(col("ARR_DELAY").alias("2018")).distinct().orderBy(col("ARR_DELAY"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("CANCELLED").alias("2009")).distinct().display()
df_2010.select(col("CANCELLED").alias("2010")).distinct().display()
df_2011.select(col("CANCELLED").alias("2011")).distinct().display()
df_2012.select(col("CANCELLED").alias("2012")).distinct().display()
df_2013.select(col("CANCELLED").alias("2013")).distinct().display()
df_2014.select(col("CANCELLED").alias("2014")).distinct().display()
df_2015.select(col("CANCELLED").alias("2015")).distinct().display()
df_2016.select(col("CANCELLED").alias("2016")).distinct().display()
df_2017.select(col("CANCELLED").alias("2017")).distinct().display()
df_2018.select(col("CANCELLED").alias("2018")).distinct().display()

# COMMAND ----------

df_2009.select(col("CANCELLATION_CODE").alias("2009")).distinct().display()
df_2010.select(col("CANCELLATION_CODE").alias("2010")).distinct().display()
df_2011.select(col("CANCELLATION_CODE").alias("2011")).distinct().display()
df_2012.select(col("CANCELLATION_CODE").alias("2012")).distinct().display()
df_2013.select(col("CANCELLATION_CODE").alias("2013")).distinct().display()
df_2014.select(col("CANCELLATION_CODE").alias("2014")).distinct().display()
df_2015.select(col("CANCELLATION_CODE").alias("2015")).distinct().display()
df_2016.select(col("CANCELLATION_CODE").alias("2016")).distinct().display()
df_2017.select(col("CANCELLATION_CODE").alias("2017")).distinct().display()
df_2018.select(col("CANCELLATION_CODE").alias("2018")).distinct().display()

# COMMAND ----------

df_2009.select(col("DIVERTED").alias("2009")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2010.select(col("DIVERTED").alias("2010")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2011.select(col("DIVERTED").alias("2011")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2012.select(col("DIVERTED").alias("2012")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2013.select(col("DIVERTED").alias("2013")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2014.select(col("DIVERTED").alias("2014")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2015.select(col("DIVERTED").alias("2015")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2016.select(col("DIVERTED").alias("2016")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2017.select(col("DIVERTED").alias("2017")).distinct().orderBy(col("DIVERTED"),ascending=False).display()
df_2018.select(col("DIVERTED").alias("2018")).distinct().orderBy(col("DIVERTED"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("CRS_ELAPSED_TIME").alias("2009")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2010.select(col("CRS_ELAPSED_TIME").alias("2010")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2011.select(col("CRS_ELAPSED_TIME").alias("2011")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2012.select(col("CRS_ELAPSED_TIME").alias("2012")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2013.select(col("CRS_ELAPSED_TIME").alias("2013")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2014.select(col("CRS_ELAPSED_TIME").alias("2014")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2015.select(col("CRS_ELAPSED_TIME").alias("2015")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2016.select(col("CRS_ELAPSED_TIME").alias("2016")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2017.select(col("CRS_ELAPSED_TIME").alias("2017")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()
df_2018.select(col("CRS_ELAPSED_TIME").alias("2018")).distinct().orderBy(col("CRS_ELAPSED_TIME"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("ACTUAL_ELAPSED_TIME").alias("2009")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2010.select(col("ACTUAL_ELAPSED_TIME").alias("2010")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2011.select(col("ACTUAL_ELAPSED_TIME").alias("2011")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2012.select(col("ACTUAL_ELAPSED_TIME").alias("2012")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2013.select(col("ACTUAL_ELAPSED_TIME").alias("2013")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2014.select(col("ACTUAL_ELAPSED_TIME").alias("2014")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2015.select(col("ACTUAL_ELAPSED_TIME").alias("2015")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2016.select(col("ACTUAL_ELAPSED_TIME").alias("2016")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2017.select(col("ACTUAL_ELAPSED_TIME").alias("2017")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()
df_2018.select(col("ACTUAL_ELAPSED_TIME").alias("2018")).distinct().orderBy(col("ACTUAL_ELAPSED_TIME"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("AIR_TIME").alias("2009")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2010.select(col("AIR_TIME").alias("2010")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2011.select(col("AIR_TIME").alias("2011")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2012.select(col("AIR_TIME").alias("2012")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2013.select(col("AIR_TIME").alias("2013")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2014.select(col("AIR_TIME").alias("2014")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2015.select(col("AIR_TIME").alias("2015")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2016.select(col("AIR_TIME").alias("2016")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2017.select(col("AIR_TIME").alias("2017")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()
df_2018.select(col("AIR_TIME").alias("2018")).distinct().orderBy(col("AIR_TIME"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("DISTANCE").alias("2009")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2010.select(col("DISTANCE").alias("2010")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2011.select(col("DISTANCE").alias("2011")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2012.select(col("DISTANCE").alias("2012")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2013.select(col("DISTANCE").alias("2013")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2014.select(col("DISTANCE").alias("2014")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2015.select(col("DISTANCE").alias("2015")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2016.select(col("DISTANCE").alias("2016")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2017.select(col("DISTANCE").alias("2017")).distinct().orderBy(col("DISTANCE"),ascending=False).display()
df_2018.select(col("DISTANCE").alias("2018")).distinct().orderBy(col("DISTANCE"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("CARRIER_DELAY").alias("2009")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2010.select(col("CARRIER_DELAY").alias("2010")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2011.select(col("CARRIER_DELAY").alias("2011")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2012.select(col("CARRIER_DELAY").alias("2012")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2013.select(col("CARRIER_DELAY").alias("2013")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2014.select(col("CARRIER_DELAY").alias("2014")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2015.select(col("CARRIER_DELAY").alias("2015")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2016.select(col("CARRIER_DELAY").alias("2016")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2017.select(col("CARRIER_DELAY").alias("2017")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()
df_2018.select(col("CARRIER_DELAY").alias("2018")).distinct().orderBy(col("CARRIER_DELAY"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("WEATHER_DELAY").alias("2009")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2010.select(col("WEATHER_DELAY").alias("2010")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2011.select(col("WEATHER_DELAY").alias("2011")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2012.select(col("WEATHER_DELAY").alias("2012")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2013.select(col("WEATHER_DELAY").alias("2013")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2014.select(col("WEATHER_DELAY").alias("2014")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2015.select(col("WEATHER_DELAY").alias("2015")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2016.select(col("WEATHER_DELAY").alias("2016")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2017.select(col("WEATHER_DELAY").alias("2017")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()
df_2018.select(col("WEATHER_DELAY").alias("2018")).distinct().orderBy(col("WEATHER_DELAY"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("NAS_DELAY").alias("2009")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2010.select(col("NAS_DELAY").alias("2010")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2011.select(col("NAS_DELAY").alias("2011")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2012.select(col("NAS_DELAY").alias("2012")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2013.select(col("NAS_DELAY").alias("2013")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2014.select(col("NAS_DELAY").alias("2014")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2015.select(col("NAS_DELAY").alias("2015")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2016.select(col("NAS_DELAY").alias("2016")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2017.select(col("NAS_DELAY").alias("2017")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()
df_2018.select(col("NAS_DELAY").alias("2018")).distinct().orderBy(col("NAS_DELAY"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("SECURITY_DELAY").alias("2009")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2010.select(col("SECURITY_DELAY").alias("2010")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2011.select(col("SECURITY_DELAY").alias("2011")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2012.select(col("SECURITY_DELAY").alias("2012")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2013.select(col("SECURITY_DELAY").alias("2013")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2014.select(col("SECURITY_DELAY").alias("2014")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2015.select(col("SECURITY_DELAY").alias("2015")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2016.select(col("SECURITY_DELAY").alias("2016")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2017.select(col("SECURITY_DELAY").alias("2017")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()
df_2018.select(col("SECURITY_DELAY").alias("2018")).distinct().orderBy(col("SECURITY_DELAY"),ascending=False).display()

# COMMAND ----------

df_2009.select(col("LATE_AIRCRAFT_DELAY").alias("2009")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2010.select(col("LATE_AIRCRAFT_DELAY").alias("2010")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2011.select(col("LATE_AIRCRAFT_DELAY").alias("2011")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2012.select(col("LATE_AIRCRAFT_DELAY").alias("2012")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2013.select(col("LATE_AIRCRAFT_DELAY").alias("2013")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2014.select(col("LATE_AIRCRAFT_DELAY").alias("2014")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2015.select(col("LATE_AIRCRAFT_DELAY").alias("2015")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2016.select(col("LATE_AIRCRAFT_DELAY").alias("2016")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2017.select(col("LATE_AIRCRAFT_DELAY").alias("2017")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()
df_2018.select(col("LATE_AIRCRAFT_DELAY").alias("2018")).distinct().orderBy(col("LATE_AIRCRAFT_DELAY"),ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Business Logic Creation

# COMMAND ----------

df = df_2009

# COMMAND ----------

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

# COMMAND ----------

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
    col("CANCELLATION_CODE"),
    when(
        col("IS_CANCELLED") == 1, coalesce(col("CANCELLATION_CODE"), lit("U"))
    ).otherwise(lit("N/A")),
)

# This keeps only "Valid" physical flights
df = df.filter((col("DISTANCE_MILES") > 0) & (col("DISTANCE_MILES").isNotNull()))

# Delay calculation
df = df.withColumn(
    col("TOTAL_DELAY_CHECKED"),
    0
    + col("DELAY_CARRIER")
    + col("DELAY_WEATHER")
    + col("DELAY_NATIONAL_AIR_SYSTEM")
    + col("DELAY_SECURITY")
    + col("DELAY_AIR_SYSTEM"),
)

df = df.withColumn(
    "IS_DELAY_DATA_CONSISTENT",
    (
        0
        + col("DELAY_CARRIER")
        + col("DELAY_WEATHER")
        + col("DELAY_NATIONAL_AIR_SYSTEM")
        + col("DELAY_SECURITY")
        + col("DELAY_AIR_SYSTEM")
    )
    == col("TOTAL_DELAY_CHECKED"),
)

df = df.withColumn(
    "ROUTE_ID", concat_ws("-", col("ORIGIN_AIRPORT"), col("DEST_AIRPORT"))
)

df = df.withColumn("_INGESTION_TIMESTAMP", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##DATA Quality Checks

# COMMAND ----------

# MAGIC %skip
# MAGIC DROP:
# MAGIC #DQ Rule 1
# MAGIC print(ORIGIN_AIRPORT IS NOT NULL AND DEST_AIRPORT IS NULL)
# MAGIC
# MAGIC #DQ Rule 2
# MAGIC print(DEST_AIRPORT IS NOT NULL AND ORIGIN_AIRPORT IS NULL)
# MAGIC
# MAGIC #DQ Rule 4
# MAGIC print(ARR_DELAY_MINUTES < 0)
# MAGIC
# MAGIC #DQ Rule 5
# MAGIC print(DEP_DELAY_MINUTES < 0)
# MAGIC
# MAGIC #DQ Rule 6
# MAGIC print(ESTIMATED_ELAPSED_TIME < 0)
# MAGIC
# MAGIC #DQ Rule 7
# MAGIC print(ACTUAL_ELAPSED_TIME < 0)
# MAGIC
# MAGIC #DQ Rule 8
# MAGIC print(AIR_TIME < 0)
# MAGIC
# MAGIC #DQ Rule 9
# MAGIC print(SCHEDULED_DEP_TIME < 0)
# MAGIC
# MAGIC #DQ Rule 10
# MAGIC print(SCHEDULED_ARR_TIME < 0)
# MAGIC
# MAGIC #DQ Rule 11
# MAGIC print(ACTUAL_DEP_TIME < 0)
# MAGIC
# MAGIC #DQ Rule 12
# MAGIC print(ACTUAL_ARR_TIME < 0)
# MAGIC
# MAGIC #DQ Rule 13
# MAGIC print(ORIGIN_AIRPORT = DEST_AIRPORT)
# MAGIC
# MAGIC #DQ Rule 14
# MAGIC print(AIRLINE_CODE IS NULL OR FLIGHT_NUMBER IS NULL)
# MAGIC
# MAGIC #DQ Rule 15
# MAGIC print(IS_CANCELLED <> 1 AND IS_DIVERTED <> 1)
# MAGIC
# MAGIC #DQ Rule 16 cancelled_arrival_null_check
# MAGIC print(IF(IS_CANCELLED = 1, ACTUAL_ARR_TIME IS NULL AND ARR_DELAY_MINUTES IS NULL AND AIR_TIME_MINUTES IS NULL, TRUE))
# MAGIC
# MAGIC #DQ Rule 17 diverted_metrics_check
# MAGIC print(IF(IS_DIVERTED = 1, ACTUAL_ARR_TIME IS NULL AND WHEELS_ON IS NULL, TRUE))
# MAGIC
# MAGIC #DQ Rule 18 Cancelled_then_no_takeoff
# MAGIC print(IF(IS_CANCELLED = 1, WHEELS_OFF IS NULL, TRUE))

# COMMAND ----------

