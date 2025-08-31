## Pré-requis
- Python 3.11+
- Java/JDK installé
- Dépendances Python : `pyspark` et `pytest`

```bash
# (optionnel) environnement virtuel
python -m venv .venv
# Windows
.venv\\Scripts\\activate
# macOS/Linux
source .venv/bin/activate
pip install pyspark pytest
```
## Exécution (local)
```bash
python pipeline.py \
  --inverter_yields data/in/inverter_yields.csv \
  --static_inverter_info data/in/static_inverter_info.csv \
  --sldc_events data/in/sldc_events.csv \
  --site_median_reference data/in/site_median_reference.csv \
  --out_dir data/out/kpi_solar \
  --n_lowest 5 \
  --lowest_dir data/out/lowest_sites \
  --broadcast_events
```
## Sorties générées

Parquet partitionné par project_code et year_month dans :
data/out/kpi_solar/

Parquet de l’agrégat "lowest N sites" dans :
data/out/lowest_sites/

## Vérification rapide
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("data/out/kpi_solar")
df.select("project_code", "year_month").distinct().show()

low = spark.read.parquet("data/out/lowest_sites")
low.show()

## Notes

--broadcast_events peut accélérer la jointure si sldc_events est de petite taille.

Les dossiers de sortie sont créés automatiquement.

La fonction get_spark() est portable (local / job Spark / Databricks) et active l’AQE par défaut.

