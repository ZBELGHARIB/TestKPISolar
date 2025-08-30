from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("SolarKPI").getOrCreate()

# 1) Lecture des CSV
yields = (spark.read.option("header", True).csv("/inverter_yields.csv", inferSchema=True))
static_info = (spark.read.option("header", True).csv("/static_inverter_info.csv", inferSchema=True))
events = (spark.read.option("header", True).csv("/sldc_events.csv", inferSchema=True))
median_ref = (spark.read.option("header", True).csv("/site_median_reference.csv", inferSchema=True))
# Cast des timestamps
for df, cols in [(yields, ["ts_start","ts_end"]), (events, ["ts_start","ts_end"]), (median_ref, ["ts_start"])]:
    for c in cols:
        df = df.withColumn(c, F.to_timestamp(c))
    if df is yields: yields = df
    elif df is events: events = df
    elif df is median_ref: median_ref = df
