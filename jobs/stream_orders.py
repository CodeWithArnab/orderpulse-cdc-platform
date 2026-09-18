"""PySpark Structured Streaming implementation of the OrderPulse pipeline.

The tested pure-Python pipeline under ``src/orderpulse`` documents the exact business
semantics. This job demonstrates how those semantics map to a distributed stream.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orderpulse.public.orders")
OUTPUT_ROOT = os.getenv("OUTPUT_ROOT", "data")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "build/checkpoints")

order_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("status", StringType()),
        StructField("amount", DecimalType(18, 2)),
        StructField("currency", StringType()),
        StructField("updated_at", StringType()),
    ]
)

source_schema = StructType(
    [
        StructField("lsn", LongType()),
        StructField("ts_ms", LongType()),
    ]
)

envelope_schema = StructType(
    [
        StructField("before", order_schema),
        StructField("after", order_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("contract_version", IntegerType()),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("orderpulse-cdc")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    raw = kafka.select(
        F.col("value").cast("string").alias("payload"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_time"),
    )

    bronze_query = (
        raw.writeStream.format("parquet")
        .option("path", f"{OUTPUT_ROOT}/bronze/orders")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze")
        .partitionBy("topic")
        .outputMode("append")
        .queryName("orderpulse_bronze")
        .start()
    )

    parsed = raw.withColumn("cdc", F.from_json("payload", envelope_schema))
    current = F.coalesce(F.col("cdc.after"), F.col("cdc.before"))
    normalized = parsed.select(
        F.sha2(
            F.concat_ws(
                ":",
                F.col("topic"),
                F.col("partition"),
                F.col("offset"),
            ),
            256,
        ).alias("event_id"),
        current["order_id"].alias("order_id"),
        current["customer_id"].alias("customer_id"),
        current["status"].alias("status"),
        current["amount"].alias("amount"),
        current["currency"].alias("currency"),
        F.to_timestamp(current["updated_at"]).alias("event_time"),
        F.col("cdc.op").alias("operation"),
        F.col("offset").alias("source_offset"),
        F.coalesce(F.col("cdc.contract_version"), F.lit(1)).alias("contract_version"),
        F.col("payload"),
    )

    valid_condition = (
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("status").isin("CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED")
        & (F.col("amount") >= 0)
        & F.col("currency").rlike("^[A-Z]{3}$")
        & F.col("operation").isin("c", "u", "d", "r")
        & F.col("event_time").isNotNull()
        & (F.col("contract_version") == 1)
    )

    quarantine = normalized.filter(~valid_condition).withColumn(
        "rejection_reason", F.lit("data contract violation")
    )
    quarantine_query = (
        quarantine.writeStream.format("parquet")
        .option("path", f"{OUTPUT_ROOT}/quarantine/orders")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/quarantine")
        .outputMode("append")
        .queryName("orderpulse_quarantine")
        .start()
    )

    silver = (
        normalized.filter(valid_condition)
        .withWatermark("event_time", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
        .drop("payload")
    )
    silver_query = (
        silver.writeStream.format("parquet")
        .option("path", f"{OUTPUT_ROOT}/silver/order_events")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver")
        .partitionBy("currency")
        .outputMode("append")
        .queryName("orderpulse_silver")
        .start()
    )

    gold = (
        silver.filter(F.col("operation") != "d")
        .groupBy(F.window("event_time", "1 minute"), "status", "currency")
        .agg(F.countDistinct("order_id").alias("order_count"), F.sum("amount").alias("gmv"))
    )
    gold_query = (
        gold.writeStream.format("parquet")
        .option("path", f"{OUTPUT_ROOT}/gold/orders_by_minute")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/gold")
        .outputMode("append")
        .queryName("orderpulse_gold")
        .start()
    )

    spark.streams.awaitAnyTermination()
    for query in (bronze_query, quarantine_query, silver_query, gold_query):
        if query.isActive:
            query.stop()


if __name__ == "__main__":
    main()
