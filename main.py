from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import os,sys

try:
    from awsglue.context import GlueContext        # Glue 3/4/5
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    IS_GLUE = True
except Exception:
    IS_GLUE = False

def get_spark():
    if IS_GLUE:
        sc = SparkContext.getOrCreate()
        glue = GlueContext(sc)
        spark = glue.spark_session
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return spark
    else:
        return (SparkSession.builder
                .appName("SolarKPI")
                .master(os.getenv("SPARK_MASTER", "local[*]"))
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .getOrCreate())

def ensure_local_dir(path: str):
    # Si on écrit sur un chemin local (file:/ ou chemin nu), on crée les dossiers parents
    if path.startswith("s3a://") or path.startswith("hdfs://") or path.startswith("dbfs:/"):
        return
    p = Path(path.replace("file:", "")).resolve()
    p.mkdir(parents=True, exist_ok=True)

def parse_args():
    from argparse import ArgumentParser, Namespace
    if IS_GLUE:
        try:
            opts = getResolvedOptions(sys.argv, ["INPUT_BASE","OUTPUT_BASE"])
            return Namespace(input_base=opts["INPUT_BASE"].rstrip("/"),
                             output_base=opts["OUTPUT_BASE"].rstrip("/"))
        except SystemExit:
            # fallback tolérant (facultatif)...
            ...
    else:
        ap = ArgumentParser()
        ap.add_argument("--input-base",  default=os.getenv("INPUT_BASE","CSVs"))
        ap.add_argument("--output-base", default=os.getenv("OUTPUT_BASE","output/solar_kpi"))
        a, _ = ap.parse_known_args()
        return Namespace(input_base=a.input_base.rstrip("/"),
                         output_base=a.output_base.rstrip("/"))


def read_inputs(spark, base):
    # Chemins d’entrée (tu peux garder inferSchema=True si tes CSVs sont propres)
    yields = (spark.read.option("header", True).csv(f"{base}/inverter_yields.csv", inferSchema=True))
    static = (spark.read.option("header", True).csv(f"{base}/static_inverter_info.csv", inferSchema=True))
    events = (spark.read.option("header", True).csv(f"{base}/sldc_events.csv", inferSchema=True))
    site_ref = (spark.read.option("header", True).csv(f"{base}/site_median_reference.csv", inferSchema=True))

    # Casts timestamp (évite les ambiguïtés plus tard)
    for df, cols in [(yields, ["ts_start","ts_end"]), (events, ["ts_start","ts_end"]), (site_ref, ["ts_start"])]:
        for c in cols:
            df = df.withColumn(c, F.to_timestamp(c))
        if df is yields: yields = df
        elif df is events: events = df
        elif df is site_ref: site_ref = df

    return yields, static, events, site_ref

def build_pipeline(yields, static, events, site_ref):
    # Aliases pour joins explicites
    y = yields.alias("y")
    s = static.alias("s")
    e = events.alias("e")
    m = site_ref.alias("m")

    # 1) Join statique (clé: logical_device_mrid ; on garde aussi project_code de y)
    df = y.join(s, on="logical_device_mrid", how="left")

    # 2) Join événements par chevauchement RÉEL d’intervalles
    #    [y.start, y.end] ∩ [e.start, e.end] ≠ ∅
    df = df.join(
        e,
        on=(
            (F.col("y.logical_device_mrid") == F.col("e.logical_device_mrid")) &
            (F.col("y.ts_start") < F.col("e.ts_end")) &
            (F.col("y.ts_end")   > F.col("e.ts_start"))
        ),
        how="left"
    )

    # 2bis) Re-sélectionner pour ne garder qu’une seule version des colonnes ambiguës
    df = df.select(
        F.col("y.*"),
        F.col("s.inverter_function_type"),
        F.col("s.storage_inverter_type"),
        F.col("s.ac_max_power"),
        F.col("e.iec63019_category_id")
    )

    # 3) Join site_median_reference sur (project_code, date(ts_start))
    df = df.withColumn("date", F.to_date("ts_start"))
    m_day = m.select(
        "project_code",
        F.to_date("ts_start").alias("date"),
        "site_median_specific_yield"
    )
    df = df.join(m_day, on=["project_code","date"], how="left")

    # 4) Calcul potentiel (10 min = 1/6 h)
    df = df.withColumn(
        "potential_production",
        F.col("specific_yield_ac") * F.col("ac_max_power") * F.lit(1.0/6.0)
    )

    # 5) Filtre Storage + DC-Coupled
    df = df.filter(
        (F.col("inverter_function_type") == F.lit("Storage")) &
        (F.col("storage_inverter_type") == F.lit("DC-Coupled"))
    )

    # 6) Partition de sortie
    df = df.withColumn("year_month", F.date_format("ts_start", "yyyy-MM"))
    return df

def write_outputs(df, out_base):
    enriched_path = f"{out_base}/enriched"
    ensure_local_dir(enriched_path)

    (df.write
       .mode("overwrite")
       .partitionBy("project_code", "year_month")
       .parquet(enriched_path))

    # === TABLE TOP-5 SITES À PLUS FAIBLE PRODUCTION ===
    top5 = (
        df.groupBy("project_code")
          .agg(F.sum("potential_production").alias("total_potential_kwh"))
          .orderBy(F.col("total_potential_kwh").asc())
          .limit(5)
    )

    top5_path = f"{out_base}/top5_lowest_sites"
    ensure_local_dir(top5_path)
    (top5.coalesce(1).write.mode("overwrite").parquet(top5_path))

    print(f"[OK] Écrit: {enriched_path} (partitionné)")
    print(f"[OK] Écrit: {top5_path}")

def main():
    args = parse_args()
    spark = get_spark()
    print(f"[INFO] INPUT_BASE={args.input_base}")
    print(f"[INFO] OUTPUT_BASE={args.output_base}")

    yields, static, events, site_ref = read_inputs(spark, args.input_base)
    print("[INFO] Counts:", { "yields":yields.count(), "static":static.count(),
                             "events":events.count(), "site_ref":site_ref.count() })

    df = build_pipeline(yields, static, events, site_ref)
    print("[INFO] Rows après filtre Storage/DC:", df.count())

    write_outputs(df, args.output_base)

    # Astuce: crée des vues si tu veux “inspecter” en Spark SQL local
    df.createOrReplaceTempView("solar_kpi_enriched")
    spark.sql("""
      SELECT project_code, SUM(potential_production) AS tot
      FROM solar_kpi_enriched GROUP BY project_code ORDER BY tot ASC LIMIT 5
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()