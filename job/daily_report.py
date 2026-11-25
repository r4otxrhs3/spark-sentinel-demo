"""Daily sales report: the Spark job this demo repository checks on every pull request.

It builds its own input (3M orders, 100k users) with spark.range, so every run sees exactly
the same data, and runs three queries:

  1. revenue by user segment and country (orders joined with the small users table)
  2. the 100 biggest spenders
  3. orders by size band

Run it locally:   python job/daily_report.py
Event log goes to $SPARK_EVENTS (default ./spark-events); results go to ./out.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ORDERS = 3_000_000
USERS = 100_000
COUNTRIES = ["IN", "US", "DE", "BR", "JP", "GB", "FR", "ID"]


def spark_session() -> SparkSession:
    events = Path(os.environ.get("SPARK_EVENTS", "spark-events")).resolve()
    shutil.rmtree(events, ignore_errors=True)  # one run = one log
    events.mkdir(parents=True)
    return (
        SparkSession.builder.appName("daily-report")
        .master("local[4]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", events.as_uri())
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def orders(spark: SparkSession):
    """Deterministic pseudo-random orders: the same rows on every run."""
    return spark.range(ORDERS).select(
        F.col("id").alias("order_id"),
        ((F.col("id") * 7919) % USERS).alias("user_id"),
        F.element_at(
            F.array(*[F.lit(c) for c in COUNTRIES]), (F.col("id") % len(COUNTRIES) + 1).cast("int")
        ).alias("country"),
        (((F.col("id") * 2654435761) % 50_000) / 100.0).alias("amount"),
    )


def users(spark: SparkSession):
    return spark.range(USERS).select(
        F.col("id").alias("user_id"),
        F.element_at(
            F.array(F.lit("new"), F.lit("regular"), F.lit("vip")),
            (F.col("id") % 3 + 1).cast("int"),
        ).alias("segment"),
    )


def size_band(amount):
    return F.when(amount < 50, "small").when(amount < 250, "medium").otherwise("large")


def main() -> None:
    spark = spark_session()
    out = Path("out")

    # 1. Revenue by segment and country
    (
        orders(spark)
        .join(users(spark), "user_id")
        .groupBy("segment", "country")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
        .write.mode("overwrite")
        .parquet(str(out / "revenue"))
    )

    # 2. The 100 biggest spenders
    (
        orders(spark)
        .groupBy("user_id")
        .agg(F.sum("amount").alias("spent"))
        .orderBy(F.desc("spent"))
        .limit(100)
        .write.mode("overwrite")
        .parquet(str(out / "top_spenders"))
    )

    # 3. Orders by size band
    (
        orders(spark)
        .select(size_band(F.col("amount")).alias("band"))
        .groupBy("band")
        .count()
        .write.mode("overwrite")
        .parquet(str(out / "bands"))
    )

    spark.stop()


if __name__ == "__main__":
    main()
