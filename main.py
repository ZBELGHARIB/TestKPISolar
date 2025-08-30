
import os
import sys
from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession

# If executed on AWS Glue, these imports are available.
try:
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    IS_GLUE = True
except Exception:
    IS_GLUE = False

def get_spark():
    if IS_GLUE:
        sc = SparkContext.getOrCreate()
        glue_ctx = GlueContext(sc)
        spark = glue_ctx.spark_session
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
        return spark
    else:
        spark = (SparkSession.builder
                 .appName("SolarKPI-GlueCompatible")
                 .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                 .getOrCreate())
        return spark