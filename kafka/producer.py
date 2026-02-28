# Write your kafka producer code here
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = (
    SparkSession.builder
    .appName("CSV-to-Kafka-Producer")
    .getOrCreate()
)

# Lecture du CSV
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/opt/spark-apps/data/transactions.csv")
)

# Kafka attend key et value en STRING ou BINARY
kafka_df = (
    df.selectExpr(
        "CAST(id AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )
)

# Envoi vers Kafka
(
    kafka_df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "transactions")
    .save()
)

print("✅ CSV envoyé vers Kafka")

spark.stop()
