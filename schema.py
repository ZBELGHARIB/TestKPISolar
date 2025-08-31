from pyspark.sql import SparkSession
import json

# 1) Démarrer Spark (local)
spark = (
    SparkSession.builder
    .appName("ReadParquet")
    .master("local[*]")
    .getOrCreate()
)

# 2) Chemin du Parquet (dossier ou fichier .parquet)
#    - local: "out/solar_kpi/enriched"
#    - S3:    "s3a://mon-bucket/chemin/enriched" (nécessite hadoop-aws/config S3)
path = "output/solar_kpi/top5_lowest_sites"

# 3) Lecture
df = spark.read.parquet(path)         # auto-discover des partitions si 'path' est un dossier

# 4) Affichages utiles
df.printSchema()                       # schéma lisible
print("Colonnes:", df.columns)         # liste des colonnes
df.show(5, truncate=False)             # aperçu des données

# Schéma en version compacte / JSON
print("Schema (compact):", df.schema.simpleString())
print(json.dumps(df.schema.jsonValue(), indent=2))

spark.stop()
