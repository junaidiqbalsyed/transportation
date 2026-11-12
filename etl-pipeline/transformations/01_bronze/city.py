SOURCE_PATH = "abfss://data@certprojects.dfs.core.windows.net/city"

from pyspark.sql.functions import current_timestamp, col
from pyspark import pipelines as dp


@dp.materialized_view(
    name="transportation.bronze.city",
    comment="City Raw Data Processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = (
        spark.
        read.
        format("csv").
        option("header", "true").
        option("inferSchema", "true").
        option("mode", "PERMISSIVE").
        option("mergeSchema", "true").
        option("columnNameOfCorruptRecord", "_corrupt_record").
        load(SOURCE_PATH)
    )
    df = (
            df.
            withColumn("file_name", col("_metadata.file_path")).
            withColumn("ingest_datetime", current_timestamp())
        )
    return df