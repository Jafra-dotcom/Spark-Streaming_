## Spark Streaming — Kafka to Spark Pipeline (Introduction Lab)



import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.schema import TRANSACTION_SCHEMA

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "transactions"
CHECKPOINT_DIR = "/tmp/spark-checkpoints/transactions"
OUTPUT_DIR = "/tmp/spark-output/transactions"

# Minimum amount (USD) kept after filtering
AMOUNT_THRESHOLD = 100.0

# Kafka ↔ Spark connector JAR (must match your Spark / Scala / Kafka versions)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


# ─── Task 2 — Create a Spark Session ─────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Build a SparkSession configured for Structured Streaming with Kafka.

    The `spark.jars.packages` config tells Spark to download (or use a local
    cache of) the Kafka connector at start-up.
    """
    spark = (
        SparkSession.builder
        .appName("KafkaTransactionStream")
        .master("local[*]")           # Use all local cores (change to spark://... for cluster)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # Keep shuffle partitions small for a local/dev setup
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─── Task 3 — Read Data from Kafka ───────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Return a *streaming* DataFrame backed by the Kafka topic.

    Kafka delivers every message as a row with these binary columns:
        key, value, topic, partition, offset, timestamp, timestampType

    The payload we care about lives in `value` (bytes).
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # consume from the beginning on first run
        .option("failOnDataLoss", "false")
        .load()
    )

    print("[Stream] Schema delivered by Kafka:")
    raw_df.printSchema()
    # root
    #  |-- key: binary
    #  |-- value: binary        ← our JSON transaction
    #  |-- topic: string
    #  |-- partition: integer
    #  |-- offset: long
    #  |-- timestamp: timestamp
    #  |-- timestampType: integer

    return raw_df


# ─── Task 4 — Parse & Inspect the Stream ────────────────────────────────────

def parse_stream(raw_df):
    """
    1. Cast the binary `value` column to a UTF-8 string.
    2. Use `from_json` + our schema to deserialise the JSON payload.
    3. Flatten nested columns with `select(col.*)`
    """
    # Step 1 — bytes → string
    string_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str")

    # Step 2 — string → structured columns
    parsed_df = string_df.select(
        F.from_json(F.col("json_str"), TRANSACTION_SCHEMA).alias("data")
    )

    # Step 3 — flatten
    transactions_df = parsed_df.select("data.*")

    print("[Stream] Parsed transaction schema:")
    transactions_df.printSchema()
    # root
    #  |-- transaction_id: string
    #  |-- user_id: string
    #  |-- amount: double
    #  |-- category: string
    #  |-- timestamp: timestamp

    return transactions_df


# ─── Task 5 — Filter the Stream ─────────────────────────────────────────────

def filter_stream(transactions_df):
    """
    Keep only high-value transactions (amount > AMOUNT_THRESHOLD).

    Observation: filter() on a streaming DataFrame is identical to
    filter() on a batch DataFrame — the Spark API is unified.
    """
    high_value_df = transactions_df.filter(
        F.col("amount") > AMOUNT_THRESHOLD
    )

    # Bonus filter: only keep specific categories
    interesting_categories = ["electronics", "travel"]
    filtered_df = high_value_df.filter(
        F.col("category").isin(interesting_categories)
    )

    return filtered_df


# ─── Task 6 — Write Streaming Results ────────────────────────────────────────

def write_to_console(df, query_name: str = "console_output"):
    """
    Sink 1 — Console (best for development & debugging).
    Spark prints each micro-batch to stdout.
    """
    return (
        df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .queryName(query_name)
        .trigger(processingTime="5 seconds")   # micro-batch every 5 s
        .start()
    )


def write_to_memory(df, query_name: str = "mem_output"):
    """
    Sink 2 — In-memory table.
    Useful for interactive inspection via spark.sql("SELECT * FROM mem_output").
    """
    return (
        df.writeStream
        .outputMode("append")
        .format("memory")
        .queryName(query_name)
        .trigger(processingTime="5 seconds")
        .start()
    )


def write_to_files(df, output_dir: str = OUTPUT_DIR, checkpoint_dir: str = CHECKPOINT_DIR):
    """
    Sink 3 — Parquet files (durable, production-ready).
    Each micro-batch is written as a new set of Parquet part-files.
    """
    return (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_dir)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="10 seconds")
        .start()
    )


# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print(" Kafka → Spark Structured Streaming  ")
    print("=" * 60)

    # Task 2
    spark = create_spark_session()
    print(f"[Spark] Version: {spark.version}")

    # Task 3
    raw_df = read_kafka_stream(spark)

    # Task 4
    transactions_df = parse_stream(raw_df)

    # Task 5
    filtered_df = filter_stream(transactions_df)

    # Task 6 — start all three sinks; choose the ones relevant to your task
    console_query = write_to_console(filtered_df, "high_value_transactions")
    # mem_query   = write_to_memory(filtered_df, "high_value_transactions")
    # file_query  = write_to_files(filtered_df)

    print("[Spark] Streaming started. Waiting for data …  (Ctrl+C to stop)")
    console_query.awaitTermination()


if __name__ == "__main__":
    main()
