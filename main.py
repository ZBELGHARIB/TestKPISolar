from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("SolarKPI").getOrCreate()

yields = (spark.read.option("header", True).csv("/inverter_yields.csv", inferSchema=True))
static_info = (spark.read.option("header", True).csv("/static_inverter_info.csv", inferSchema=True))
events = (spark.read.option("header", True).csv("/sldc_events.csv", inferSchema=True))
median_ref = (spark.read.option("header", True).csv("/site_median_reference.csv", inferSchema=True))


for df, cols in [(yields, ["ts_start","ts_end"]), (events, ["ts_start","ts_end"]), (median_ref, ["ts_start"])]:
    for c in cols:
        df = df.withColumn(c, F.to_timestamp(c))
    if df is yields: yields = df
    elif df is events: events = df
    elif df is median_ref: median_ref = df

df = yields.join(static_info, "logical_device_mrid", "left")

df = df.join(
    events,
    on=[
        df.logical_device_mrid == events.logical_device_mrid,
        (df.ts_start >= events.ts_start) & (df.ts_start <= events.ts_end)
    ],
    how="left"
)

