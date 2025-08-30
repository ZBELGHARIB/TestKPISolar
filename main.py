from pyspark.sql import SparkSession, functions as F, types as T
import os
from pathlib import Path

out_base = os.path.abspath("output/solar_kpi")
Path(out_base).mkdir(parents=True, exist_ok=True)
spark = SparkSession.builder.appName("SolarKPI").getOrCreate()

yields = (spark.read.option("header", True).csv("CSVs/inverter_yields.csv", inferSchema=True))
static_info = (spark.read.option("header", True).csv("CSVs/static_inverter_info.csv", inferSchema=True))
events = (spark.read.option("header", True).csv("CSVs/sldc_events.csv", inferSchema=True))
median_ref = (spark.read.option("header", True).csv("CSVs/site_median_reference.csv", inferSchema=True))


for df, cols in [(yields, ["ts_start","ts_end"]), (events, ["ts_start","ts_end"]), (median_ref, ["ts_start"])]:
    for c in cols:
        df = df.withColumn(c, F.to_timestamp(c))
    if df is yields: yields = df
    elif df is events: events = df
    elif df is median_ref: median_ref = df

from pyspark.sql import functions as F

# Aliases
y = yields.alias("y")
s = static_info.alias("s")
e = events.alias("e")
m = median_ref.alias("m")

# 1) Join yields + static_info
df = y.join(s, "logical_device_mrid", "left")

# 2) Join events par chevauchement réel (intervalle qui s'intersecte)
df = df.join(
    e,
    on=(
        (F.col("y.logical_device_mrid") == F.col("e.logical_device_mrid")) &
        (F.col("y.ts_start") < F.col("e.ts_end")) &
        (F.col("y.ts_end")   > F.col("e.ts_start"))
    ),
    how="left"
)

# 2bis) IMPORTANT : ne garde qu'une seule version des colonnes ambiguës
# Ici on garde toutes les colonnes de yields (y.*), et seulement les colonnes utiles des autres tables
df = df.select(
    F.col("y.*"),                                  # ts_start/ts_end = version yields
    F.col("s.inverter_function_type"),
    F.col("s.storage_inverter_type"),
    F.col("s.ac_max_power"),
    F.col("e.iec63019_category_id")
)

# 3) Tu peux maintenant dériver 'date' sans ambiguïté
df = df.withColumn("date", F.to_date(F.col("ts_start")))

# 4) Join avec la ref journalière (on forme 'date' côté ref aussi)
m_day = m.select(
    "project_code",
    F.to_date("ts_start").alias("date"),
    "site_median_specific_yield"
)
df = df.join(m_day, on=["project_code","date"], how="left")

# 5) Suite du traitement…
df = df.withColumn(
    "potential_production",
    F.col("specific_yield_ac") * F.col("ac_max_power") * F.lit(1/6.0)
).filter(
    (F.col("inverter_function_type") == "Storage") &
    (F.col("storage_inverter_type") == "DC-Coupled")
).withColumn(
    "year_month", F.date_format("ts_start", "yyyy-MM")
)
(df.write
   .mode("overwrite")
   .partitionBy("project_code","year_month")
   .parquet(out_base))