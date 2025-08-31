import math
import pytest
from pathlib import Path
from pyspark.sql import Row, functions as F
from pipeline import (
    get_spark, write_parquet,
    prepare_inputs, join_static, join_events, join_site_reference,
    compute_potential_production, filter_storage_dc_coupled,
    add_year_month, select_final_columns, lowest_sites, run
)


@pytest.fixture(scope="session")
def spark():
    sp = get_spark(app_name="edf_tests", master="local[2]", disable_windows_native=True)
    yield sp

# ---------- Tests de la session portable ----------

def test_get_spark_configs(spark):
    jconf = spark.sparkContext._jsc.hadoopConfiguration()
    assert jconf.get("fs.s3a.impl") == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert spark.version


# ---------- Tests I/O Parquet génériques ----------

def test_write_parquet_partitioned_and_readback(tmp_path, spark):
    df = spark.createDataFrame([
        Row(project_code="ALPH", year_month="2025-08", v=1),
        Row(project_code="BETA", year_month="2025-08", v=2),
        Row(project_code="ALPH", year_month="2025-09", v=3),
    ])
    out_dir = (tmp_path / "out_parquet").as_posix()
    write_parquet(df, out_dir, partition_by=["project_code", "year_month"])

    df_back = spark.read.parquet(out_dir)
    parts = {tuple(r) for r in df_back.select("project_code", "year_month").distinct().collect()}
    assert parts == {("ALPH", "2025-08"), ("ALPH", "2025-09"), ("BETA", "2025-08")}


# ---------- Tests préparation / jointures ----------

def test_prepare_inputs_renames_and_types(spark):
    iy_raw = spark.createDataFrame([
        Row(logical_device_mrid="inv1", ts_start="2025-08-26 10:00:00", ts_end="2025-08-26 10:10:00",
            specific_yield_ac="0.5", project_code="ALPH")
    ])
    si_raw = spark.createDataFrame([Row(logical_device_mrid="inv1", ac_max_power="60000",
                                        inverter_function_type="Storage", storage_inverter_type="DC-Coupled")])
    ev_raw = spark.createDataFrame([Row(logical_device_mrid="inv1", ts_start="2025-08-26 10:05:00",
                                        ts_end="2025-08-26 10:20:00", iec63019_category_id=10001)])
    sr_raw = spark.createDataFrame([Row(project_code="ALPH", ts_start="2025-08-26 00:00:00",
                                        site_median_specific_yield=0.8)])

    iy, si, ev, sr = prepare_inputs(iy_raw, si_raw, ev_raw, sr_raw)
    assert "iy_ts_start" in iy.columns and "iy_ts_end" in iy.columns
    assert dict(iy.dtypes)["specific_yield_ac"] == "double"
    assert {"ev_mrid", "ev_start", "ev_end", "iec63019_category_id"} <= set(ev.columns)


def test_join_events_overlap_strict(spark):
    # 10:00-10:10 (mesure) ; events: [10:05,10:20) chevauche ; [10:10,10:15) ne chevauche PAS (strict)
    iy = (spark.createDataFrame([
        Row(logical_device_mrid="inv1", project_code="ALPH",
            iy_ts_start="2025-08-26 10:00:00", iy_ts_end="2025-08-26 10:10:00",
            specific_yield_ac=1.0, ac_max_power=1000.0)])
          .withColumn("iy_ts_start", F.to_timestamp("iy_ts_start"))
          .withColumn("iy_ts_end", F.to_timestamp("iy_ts_end")))

    si = spark.createDataFrame([Row(logical_device_mrid="inv1",
                                    inverter_function_type="Storage",
                                    storage_inverter_type="DC-Coupled",
                                    ac_max_power=1000.0)])

    ev = spark.createDataFrame([
        Row(ev_mrid="inv1", ev_start="2025-08-26 10:05:00", ev_end="2025-08-26 10:20:00", iec63019_category_id=10001),
        Row(ev_mrid="inv1", ev_start="2025-08-26 10:10:00", ev_end="2025-08-26 10:15:00", iec63019_category_id=10002),
    ]).withColumn("ev_start", F.to_timestamp("ev_start")) \
        .withColumn("ev_end", F.to_timestamp("ev_end"))

    df = join_static(iy, si)
    df = join_events(df, ev, use_broadcast=True)

    cats = [r["iec63019_category_id"] for r in df.select("iec63019_category_id").collect() if r[0] is not None]
    assert cats == [10001]  # seulement le chevauchement strict


def test_join_site_reference_on_day(spark):
    df = spark.createDataFrame([
        Row(project_code="ALPH", ts_start="2025-08-26 08:10:00")
    ]).withColumn("ts_start", F.to_timestamp("ts_start"))

    sr = spark.createDataFrame([
        Row(project_code="ALPH", ts_start="2025-08-26 00:00:00", site_median_specific_yield=0.857)
    ]).withColumn("ts_start", F.to_timestamp("ts_start"))

    out = join_site_reference(df, sr)
    assert math.isclose(out.select("site_median_specific_yield").first()[0], 0.857, rel_tol=1e-9)


# ---------- Tests calcul / filtre / colonnes ----------

def test_compute_filter_add_year_month_and_select(spark):
    df = spark.createDataFrame([
        Row(ts_start="2025-08-26 08:10:00", ts_end="2025-08-26 08:20:00",
            project_code="ALPH", logical_device_mrid="inv1",
            inverter_function_type="  Storage ", storage_inverter_type="dc-coupled",
            specific_yield_ac=0.5, ac_max_power=60000.0)
    ]).withColumn("ts_start", F.to_timestamp("ts_start")) \
        .withColumn("ts_end", F.to_timestamp("ts_end"))

    df = compute_potential_production(df)
    df = filter_storage_dc_coupled(df)
    df = add_year_month(df)
    df = select_final_columns(df)

    val = df.select("potential_production").first()[0]
    assert math.isclose(val, 5000.0, rel_tol=1e-9)  # 0.5 * 60000 * 1/6
    assert df.select("year_month").first()[0] == "2025-08"


# ---------- Tests agrégat lowest N ----------

def test_lowest_sites_recomputes_and_sorts(spark):
    df = spark.createDataFrame([
        Row(project_code="A", specific_yield_ac=1.0, ac_max_power=60000.0),
        Row(project_code="B", specific_yield_ac=0.1, ac_max_power=60000.0),
        Row(project_code="C", specific_yield_ac=0.5, ac_max_power=60000.0),
    ])
    low = lowest_sites(df, n=2).collect()
    codes = [r["project_code"] for r in low]
    assert codes == ["B", "C"]


# ---------- Test orchestrateur end-to-end ----------

def _write_csv_text(path: Path, header: str, rows: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for r in rows:
            f.write(r + "\n")


def test_run_end_to_end(tmp_path, spark):
    # Petits CSV temporaires
    inv_p = tmp_path / "inverter_yields.csv"
    si_p = tmp_path / "static_inverter_info.csv"
    ev_p = tmp_path / "sldc_events.csv"
    sr_p = tmp_path / "site_median_reference.csv"

    _write_csv_text(inv_p,
                    "ts_start,ts_end,logical_device_mrid,logical_device_name,project_code,project_mrid,specific_yield_ac,reference_yield_stc",
                    ["2025-08-26 10:00:00,2025-08-26 10:10:00,inv1,Inverter 1,ALPH,pmrid_1,0.5,1.0"]
                    )
    _write_csv_text(si_p,
                    "logical_device_mrid,inverter_function_type,storage_inverter_type,ac_max_power",
                    ["inv1,Storage,DC-Coupled,60000"]
                    )
    _write_csv_text(ev_p,
                    "ts_start,ts_end,logical_device_mrid,iec63019_category_id",
                    ["2025-08-26 10:05:00,2025-08-26 10:20:00,inv1,10001",
                     "2025-08-26 10:10:00,2025-08-26 10:15:00,inv1,10002"]  # non-chevauchement strict
                    )
    _write_csv_text(sr_p,
                    "ts_start,project_code,site_median_specific_yield",
                    ["2025-08-26 00:00:00,ALPH,0.8"]
                    )

    out_dir = (tmp_path / "out_parquet").as_posix()
    lowest_dir = (tmp_path / "lowest_parquet").as_posix()

    df_final = run(
        spark,
        inverter_yields_path=inv_p.as_posix(),
        static_inverter_info_path=si_p.as_posix(),
        sldc_events_path=ev_p.as_posix(),
        site_median_reference_path=sr_p.as_posix(),
        out_dir=out_dir,
        n_lowest=5,
        lowest_dir=lowest_dir,
        use_broadcast_events=True,
    )

    # Le DF final doit contenir notre ligne
    assert df_final.count() == 1
    row = df_final.select("project_code", "potential_production", "year_month").first()
    assert row["project_code"] == "ALPH"
    assert math.isclose(row["potential_production"], 5000.0, rel_tol=1e-9)
    assert row["year_month"] == "2025-08"

    # Vérifie les partitions écrites
    df_back = spark.read.parquet(out_dir)
    parts = {tuple(r) for r in df_back.select("project_code", "year_month").distinct().collect()}
    assert parts == {("ALPH", "2025-08")}

    # Vérifie le parquet "lowest"
    low_back = spark.read.parquet(lowest_dir)
    assert low_back.count() == 1
    low_row = low_back.first()
    assert low_row["project_code"] == "ALPH"
    assert math.isclose(low_row["total_potential_production"], 5000.0, rel_tol=1e-9)
