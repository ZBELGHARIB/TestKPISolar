import os, sys
from datetime import datetime
from pathlib import Path
from pyspark.sql import functions as F, types as T
import main as job 

def _ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

def test_parse_args_cli_vs_env(monkeypatch, tmp_path):
    # ENV par défaut
    monkeypatch.setenv("INPUT_BASE",  "ENV_IN")
    monkeypatch.setenv("OUTPUT_BASE", "ENV_OUT")
    sys.argv = ["edf_energies.py"]
    args = job.parse_args()
    assert args.input_base == "ENV_IN"
    assert args.output_base == "ENV_OUT"

    # CLI doit primer sur ENV
    sys.argv = ["edf_energies.py", "--input-base", "CLI_IN", "--output-base", "CLI_OUT"]
    args = job.parse_args()
    assert args.input_base == "CLI_IN"
    assert args.output_base == "CLI_OUT"

def test_build_pipeline_happy_path(spark):
    # Yields (2 lignes, 2 projets)
    yields = spark.createDataFrame([
        ("INV1","Inv1","ALPH", _ts("2025-05-01 10:00:00"), _ts("2025-05-01 10:10:00"), 0.8, 0.7),
        ("INV2","Inv2","BETA", _ts("2025-06-15 12:00:00"), _ts("2025-06-15 12:10:00"), 0.5, 0.6),
    ], schema=T.StructType()
        .add("logical_device_mrid","string")
        .add("logical_device_name","string")
        .add("project_code","string")
        .add("ts_start","timestamp")
        .add("ts_end","timestamp")
        .add("specific_yield_ac","double")
        .add("reference_yield_stc","double"))

    static = spark.createDataFrame([
        ("INV1","Storage","DC-Coupled",50000.0,"ALPH"),
        ("INV2","Storage","DC-Coupled",35000.0,"BETA"),
    ], "logical_device_mrid string, inverter_function_type string, storage_inverter_type string, ac_max_power double, project_code string")

    events = spark.createDataFrame([
        ("INV1", _ts("2025-05-01 09:59:00"), _ts("2025-05-01 10:11:00"), 10004),  # chevauche
        ("INV2", _ts("2025-06-15 11:00:00"), _ts("2025-06-15 11:30:00"), 10004),  # ne chevauche pas
    ], "logical_device_mrid string, ts_start timestamp, ts_end timestamp, iec63019_category_id int")

    site_ref = spark.createDataFrame([
        ("ALPH", _ts("2025-05-01 00:00:00"), 0.72),
        ("BETA", _ts("2025-06-15 00:00:00"), 0.55),
    ], "project_code string, ts_start timestamp, site_median_specific_yield double")

    df = job.build_pipeline(yields, static, events, site_ref)

    # 1) année-mois
    rows = {r["project_code"]: r["year_month"] for r in df.select("project_code","year_month").collect()}
    assert rows == {"ALPH":"2025-05", "BETA":"2025-06"}

    # 2) calcul potentiel
    m = {r["project_code"]: r["potential_production"] for r in df.select("project_code","potential_production").collect()}
    assert abs(m["ALPH"] - (0.8*50000/6.0)) < 1e-6
    assert abs(m["BETA"] - (0.5*35000/6.0)) < 1e-6

    # 3) event (chevauchement réel uniquement)
    assert df.filter(F.col("iec63019_category_id").isNotNull()).count() == 1

    # 4) join median ref
    assert df.filter(F.col("site_median_specific_yield").isNotNull()).count() == 2

def test_overlap_strict_boundaries(spark):
    yields = spark.createDataFrame([
        ("INV1","Name","P", _ts("2025-07-01 10:00:00"), _ts("2025-07-01 10:10:00"), 0.1, 0.1),
    ], "logical_device_mrid string, logical_device_name string, project_code string, ts_start timestamp, ts_end timestamp, specific_yield_ac double, reference_yield_stc double")

    static = spark.createDataFrame([
        ("INV1","Storage","DC-Coupled",1000.0,"P"),
    ], "logical_device_mrid string, inverter_function_type string, storage_inverter_type string, ac_max_power double, project_code string")

    # deux évènements en bordure (doivent être ignorés), un seul vrai chevauchement
    events = spark.createDataFrame([
        ("INV1", _ts("2025-07-01 09:00:00"), _ts("2025-07-01 10:00:00"), 1),
        ("INV1", _ts("2025-07-01 10:10:00"), _ts("2025-07-01 11:00:00"), 2),
        ("INV1", _ts("2025-07-01 09:59:00"), _ts("2025-07-01 10:11:00"), 3),
    ], "logical_device_mrid string, ts_start timestamp, ts_end timestamp, iec63019_category_id int")

    site_ref = spark.createDataFrame([
        ("P", _ts("2025-07-01 00:00:00"), 0.1),
    ], "project_code string, ts_start timestamp, site_median_specific_yield double")

    df = job.build_pipeline(yields, static, events, site_ref)
    assert df.count() == 1
    assert df.select("iec63019_category_id").first()[0] == 3

def test_write_outputs_creates_parquet_and_top5(spark, tmp_path):
    yields = spark.createDataFrame([
        ("INV1","Name","A", _ts("2025-01-01 00:00:00"), _ts("2025-01-01 00:10:00"), 0.2, 0.1),
        ("INV2","Name","B", _ts("2025-01-01 01:00:00"), _ts("2025-01-01 01:10:00"), 0.4, 0.1),
        ("INV3","Name","C", _ts("2025-01-01 02:00:00"), _ts("2025-01-01 02:10:00"), 0.1, 0.1),
    ], "logical_device_mrid string, logical_device_name string, project_code string, ts_start timestamp, ts_end timestamp, specific_yield_ac double, reference_yield_stc double")

    static = spark.createDataFrame([
        ("INV1","Storage","DC-Coupled",10000.0,"A"),
        ("INV2","Storage","DC-Coupled",10000.0,"B"),
        ("INV3","Storage","DC-Coupled",10000.0,"C"),
    ], "logical_device_mrid string, inverter_function_type string, storage_inverter_type string, ac_max_power double, project_code string")

    events = spark.createDataFrame([], "logical_device_mrid string, ts_start timestamp, ts_end timestamp, iec63019_category_id int")
    site_ref = spark.createDataFrame([
        ("A", _ts("2025-01-01 00:00:00"), 0.2),
        ("B", _ts("2025-01-01 00:00:00"), 0.2),
        ("C", _ts("2025-01-01 00:00:00"), 0.2),
    ], "project_code string, ts_start timestamp, site_median_specific_yield double")

    df = job.build_pipeline(yields, static, events, site_ref)

    out = tmp_path / "out"
    job.write_outputs(df, str(out))

    # Dossiers créés
    assert (out / "enriched").is_dir()
    assert (out / "top5_lowest_sites").is_dir()

    # Top-5 correct et ordonné
    top5 = spark.read.parquet(str(out / "top5_lowest_sites"))
    rows = [ (r["project_code"], r["total_potential_kwh"]) for r in top5.orderBy("total_potential_kwh").collect() ]
    assert [r[0] for r in rows] == ["C","A","B"]
    exp = {"A": 0.2*10000/6, "B": 0.4*10000/6, "C": 0.1*10000/6}
    for code, val in rows:
        assert abs(val - exp[code]) < 1e-6

def test_ensure_local_dir(tmp_path):
    loc = tmp_path / "nested/dir"
    assert not loc.exists()
    job.ensure_local_dir(str(loc))
    assert loc.exists() and loc.is_dir()

    # s3a:// ne doit pas créer en local ni lever d'erreur
    job.ensure_local_dir("s3a://bucket/prefix")
