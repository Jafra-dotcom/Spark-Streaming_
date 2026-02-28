from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum
from utils.schema import transaction_schema

spark = SparkSession.builder \
    .appName("Kafka-Spark-Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lire depuis Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka value (bytes) → string → JSON
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

# Agrégation
agg_df = parsed_df.groupBy("user_id").agg(sum("amount").alias("total_amount"))


# Affichage en streaming
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

