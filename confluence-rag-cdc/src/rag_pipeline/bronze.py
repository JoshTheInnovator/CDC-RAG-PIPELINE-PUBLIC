"""Raw S3 JSON events -> bronze Delta table (append-only change log, full doc body)."""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from .common import parse_args

RAW_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),     # page_upserted | page_deleted
    StructField("source_event", StringType()),   # original Confluence event name
    StructField("received_at", StringType()),
    StructField("page_id", StringType()),
    StructField("space_key", StringType()),
    StructField("title", StringType()),
    StructField("version", IntegerType()),
    StructField("last_modified", StringType()),
    StructField("url", StringType()),
    StructField("body_html", StringType()),
])


def main(argv=None):
    cfg = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(RAW_SCHEMA)
        .load(cfg.raw_path)
        .withColumn("received_at", F.to_timestamp("received_at"))
        .withColumn("last_modified", F.to_timestamp("last_modified"))
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    (
        stream.writeStream
        .option("checkpointLocation", f"{cfg.checkpoint_root}/bronze")
        .trigger(availableNow=True)
        .toTable(cfg.bronze_table)
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
