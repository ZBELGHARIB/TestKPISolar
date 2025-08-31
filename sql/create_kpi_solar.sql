CREATE DATABASE IF NOT EXISTS energy;

CREATE EXTERNAL TABLE IF NOT EXISTS energy.kpi_solar (
  ts_start timestamp,
  ts_end timestamp,
  ts_day timestamp,
  logical_device_mrid string,
  logical_device_name string,
  project_mrid string,
  specific_yield_ac double,
  reference_yield_stc double,
  inverter_function_type string,
  storage_inverter_type string,
  ac_max_power double,
  iec63019_category_id int,
  site_median_specific_yield double,
  potential_production double
)
PARTITIONED BY (
  project_code string,
  year_month string
)
STORED AS PARQUET
LOCATION 's3://data/out/kpi_solar'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY'
);

-- Refresh partitions written by Spark
MSCK REPAIR TABLE energy.fct_storage_inverter_production;
