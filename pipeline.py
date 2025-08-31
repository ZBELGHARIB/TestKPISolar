from __future__ import annotations
import os
import logging
from typing import Tuple, Optional, Union, Iterable, Dict
from pyspark.sql import SparkSession, DataFrame, functions as F

LOG_LEVEL = os.getenv("PY_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger("edf.pipeline")


# --------------------------
# Session Spaek et IO
# --------------------------

def get_spark(
        app_name: str = "edf_pipeline",
        master: Optional[str] = None,
        enable_adaptive: bool = True,
        auto_env: bool = True,  # déduire local si pas Databricks
        disable_windows_native: Optional[bool] = None,  # auto: True sur Windows local
        extra_spark_conf: Optional[Dict[str, str]] = None,
        extra_hadoop_conf: Optional[Dict[str, str]] = None,
) -> SparkSession:
    is_databricks = any(k in os.environ for k in ("DATABRICKS_RUNTIME_VERSION", "DB_IS_DRIVER"))

    if auto_env and master is None and not is_databricks:
        master = os.environ.get("SPARK_MASTER", "local[*]")

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    # AQE (adaptive) sans imposer spark.sql.shuffle.partitions
    if enable_adaptive:
        builder = (builder
                   .config("spark.sql.adaptive.enabled", "true")
                   .config("spark.sql.adaptive.coalescePartitions.enabled", "true"))

    # Confs additionnelles si besoin
    if extra_spark_conf:
        for k, v in extra_spark_conf.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    logger.info("Spark %s (master=%s, appId=%s)",
        spark.version, spark.sparkContext.master, spark.sparkContext.applicationId
    )

    if extra_hadoop_conf:
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        for k, v in extra_hadoop_conf.items():
            hconf.set(k, v)
        logger.debug("Applied extra Hadoop conf keys: %s", list(extra_hadoop_conf.keys()))
    return spark


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def write_parquet(
        df: DataFrame,
        out_dir: str,
        partition_by: Optional[Union[str, Iterable[str]]] = None,
        mode: str = "overwrite",
        coalesce: Optional[int] = None,
        compression: Optional[str] = None,
) -> None:
    """
    Écrit un DataFrame en Parquet.
    - partition_by: None, une str, ou une liste de colonnes pour partitionner
    - mode: 'overwrite' | 'append'
    - coalesce: nombre de fichiers de sortie
    - compression: ex. 'snappy', 'gzip', etc.
    """
    # Réduire le nombre de fichiers
    base_df = df.coalesce(coalesce) if coalesce else df

    # Validation des colonnes de partition
    if partition_by:
        if isinstance(partition_by, str):
            partition_cols = [partition_by]
        else:
            partition_cols = list(partition_by)
        missing = [c for c in partition_cols if c not in base_df.columns]
        if missing:
            raise ValueError(f"Colonnes de partition manquantes dans le DF: {missing}")
    else:
        partition_cols = []

    writer = base_df.write.mode(mode)
    if compression:
        writer = writer.option("compression", compression)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(out_dir)


# --------------------------
# Préparation des entrées
# --------------------------

def prepare_inputs(
        df_inverter_yields: DataFrame,
        df_static_inverter_info: DataFrame,
        df_sldc_events: DataFrame,
        df_site_median_reference: DataFrame,
) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    # Typage et renommage des colonnes pour eviter toute ambiguité
    iy = (df_inverter_yields
          .withColumn("ts_start", F.to_timestamp("ts_start"))
          .withColumn("ts_end", F.to_timestamp("ts_end"))
          .withColumn("specific_yield_ac", F.col("specific_yield_ac").cast("double"))
          .withColumnRenamed("ts_start", "iy_ts_start")
          .withColumnRenamed("ts_end", "iy_ts_end"))

    si = (df_static_inverter_info
          .withColumn("ac_max_power", F.col("ac_max_power").cast("double")))

    ev = (df_sldc_events
    .select(
        F.col("logical_device_mrid").alias("ev_mrid"),
        F.to_timestamp("ts_start").alias("ev_start"),
        F.to_timestamp("ts_end").alias("ev_end"),
        "iec63019_category_id"
    ))

    sr = (df_site_median_reference
          .withColumn("ts_start", F.to_timestamp("ts_start")))

    return iy, si, ev, sr


# --------------------------
# Transformations
# --------------------------

def join_static(iy: DataFrame, si: DataFrame) -> DataFrame:
    return iy.join(si, on="logical_device_mrid", how="left")


def join_events(
        df: DataFrame,
        ev: DataFrame,
        use_broadcast: bool = False,
        iy_col_start: str = "iy_ts_start",
        iy_col_end: str = "iy_ts_end",
) -> DataFrame:
    """Join par chevauchement temporel + même onduleur."""
    left = df.alias("iy")
    right = ev.alias("ev")

    cond = (
            (left["logical_device_mrid"] == right["ev_mrid"]) &
            (left[iy_col_start] < right["ev_end"]) &
            (left[iy_col_end] > right["ev_start"])
    )

    right_df = F.broadcast(right) if use_broadcast else right
    joined_df = left.join(right_df, cond, "left")
    # Recrée ts_start/ts_end  à partir des colonnes mesures
    joined_df = (joined_df
                 .withColumn("ts_start", F.col(f"iy.{iy_col_start}"))
                 .withColumn("ts_end", F.col(f"iy.{iy_col_end}")))
    return joined_df


def join_site_reference(df: DataFrame, sr: DataFrame) -> DataFrame:
    """Join référence site sur (project_code, ts_day) / ts_day = date_trunc(DAY, ts_start)."""
    df_days = df.withColumn("ts_day", F.date_trunc("DAY", F.col("ts_start")))
    sr_days = (sr.withColumn("ts_day", F.date_trunc("DAY", F.col("ts_start")))
               .select("project_code", "ts_day", "site_median_specific_yield"))
    return df_days.join(sr_days, on=["project_code", "ts_day"], how="left")


def compute_potential_production(df: DataFrame) -> DataFrame:
    """potential_production = specific_yield_ac × ac_max_power × 1/6."""
    return (df.withColumn("potential_production",
                          (F.coalesce(F.col("specific_yield_ac"), F.lit(0.0)) *
                           F.coalesce(F.col("ac_max_power"), F.lit(0.0)) *
                           F.lit(1.0 / 6.0)).cast("double"))
            .withColumn("potential_production", F.round("potential_production", 6)))


def filter_storage_dc_coupled(df: DataFrame) -> DataFrame:
    """Filtrage de Storage + DC-Coupled (robuste à la casse/espaces)."""
    return df.where(
        (F.lower(F.trim(F.coalesce(F.col("inverter_function_type"), F.lit("")))) == F.lit("storage")) &
        (F.lower(F.trim(F.coalesce(F.col("storage_inverter_type"), F.lit("")))) == F.lit("dc-coupled"))
    )


def add_year_month(df: DataFrame, from_ts: str = "ts_start") -> DataFrame:
    return df.withColumn("year_month", F.date_format(F.col(from_ts), "yyyy-MM"))


def select_final_columns(df: DataFrame) -> DataFrame:
    cols = [
        "ts_start", "ts_end", "ts_day",
        "logical_device_mrid", "logical_device_name",
        "project_code", "project_mrid",
        "specific_yield_ac", "reference_yield_stc",
        "inverter_function_type", "storage_inverter_type", "ac_max_power",
        "iec63019_category_id", "site_median_specific_yield",
        "potential_production", "year_month",
    ]
    return df.select(*[c for c in cols if c in df.columns])


# --------------------------
# Agrégats "lowest 5"
# --------------------------

def lowest_sites(df: DataFrame, n: int = 5) -> DataFrame:
    """
    Table triée des n sites avec la plus faible production totale.
    """
    if "potential_production" not in df.columns:
        df = compute_potential_production(df)

    return (
        df.groupBy("project_code")
        .agg(F.sum("potential_production").alias("total_potential_production"))
        .orderBy(F.col("total_potential_production").asc_nulls_last(),
                 F.col("project_code").asc())
        .limit(n)
    )


# --------------------------
# Orchestrateur
# --------------------------

def run(
        spark: SparkSession,
        inverter_yields_path: str,
        static_inverter_info_path: str,
        sldc_events_path: str,
        site_median_reference_path: str,
        out_dir: str,
        n_lowest: int,
        lowest_dir: str,
        use_broadcast_events: bool = False,
) -> DataFrame:
    # 1) Lécture des 4 jeux de données CSV
    logger.info("Reading inputs: %s, %s, %s, %s",
                inverter_yields_path, static_inverter_info_path, sldc_events_path, site_median_reference_path)
    iy_raw = read_csv(spark, inverter_yields_path)
    si_raw = read_csv(spark, static_inverter_info_path)
    ev_raw = read_csv(spark, sldc_events_path)
    sr_raw = read_csv(spark, site_median_reference_path)

    iy, si, ev, sr = prepare_inputs(iy_raw, si_raw, ev_raw, sr_raw)
    # 2) Jointure de 4 dataframe
    logger.info("Joining static info...")
    df = join_static(iy, si)
    logger.info("Joining events (broadcast=%s)...", use_broadcast_events)
    df = join_events(df, ev, use_broadcast=use_broadcast_events)
    logger.info("Joining site reference (by project_code + ts_day)...")
    df = join_site_reference(df, sr)
    # 3) Calcule potential_production = specific_yield_ac × ac_max_power × 1/6 (10min en heures)
    logger.info("Computing potential_production, filtering Storage/DC-Coupled, adding year_month...")
    df = compute_potential_production(df)
    # 4) Conservation des inverters "Storage" qui sont "DC-Coupled"
    df = filter_storage_dc_coupled(df)
    # 5) Production d'un fichier parquet partitionné par project_code et year_month
    df = add_year_month(df)

    df = select_final_columns(df)

    logger.info("Writing KPI parquet to %s (partitioned by project_code,year_month)", out_dir)
    write_parquet(df, out_dir, partition_by=["project_code", "year_month"])

    # Création d'une table triée des 5 sites avec la plus faible production totale
    low_df = lowest_sites(df, n=n_lowest)

    logger.info("Writing lowest %d sites to %s", n_lowest, lowest_dir)
    write_parquet(low_df, lowest_dir)

    logger.info("Sample rows after processing:")
    # df.show(10, truncate=False)
    # low_df.show(10, truncate=False)

    return df


# --------------------------
# CLI
# --------------------------

if __name__ == "__main__":
    import argparse
    logger.info("Job start ...")
    parser = argparse.ArgumentParser(description="Spark pipeline - test EDF")
    parser.add_argument("--inverter_yields", required=True)
    parser.add_argument("--static_inverter_info", required=True)
    parser.add_argument("--sldc_events", required=True)
    parser.add_argument("--site_median_reference", required=True)
    parser.add_argument("--out_dir", required=True)
    parser.add_argument("--broadcast_events", action="store_true")
    parser.add_argument("--n_lowest", type=int, default=5, help="Nombre de sites à retourner (défaut: 5)")
    parser.add_argument("--lowest_dir", help="Chemin dossier Parquet pour écrire le tableau lowest N")

    args = parser.parse_args()
    spark = get_spark(app_name="edf_test")
    final_df = run(
        spark,
        inverter_yields_path=args.inverter_yields,
        static_inverter_info_path=args.static_inverter_info,
        sldc_events_path=args.sldc_events,
        site_median_reference_path=args.site_median_reference,
        out_dir=args.out_dir,
        use_broadcast_events=args.broadcast_events,
        n_lowest=args.n_lowest,
        lowest_dir=args.lowest_dir
    )
    logger.info("Job end.")
