from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("SolarKPI").getOrCreate()

# 1) Lecture des CSV
yields = (spark.read.option("header", True).csv("/path/inverter_yields.csv", inferSchema=True))
static_info = (spark.read.option("header", True).csv("/path/static_inverter_info.csv", inferSchema=True))
events = (spark.read.option("header", True).csv("/path/sldc_events.csv", inferSchema=True))
median_ref = (spark.read.option("header", True).csv("/path/site_median_reference.csv", inferSchema=True))
