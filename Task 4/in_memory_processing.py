# src/in_memory_processing.py
from pyspark.sql import SparkSession
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("InMemoryDataProcessing") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Load dataset
data_path = "../data/large_dataset.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Persist in memory for faster repeated processing
df.cache()

# Show initial data
print("Initial Data:")
df.show(5)

# Measure performance for an aggregation (disk vs memory)
start_disk = time.time()
df.unpersist()  # remove cache to simulate disk-based
result_disk = df.groupBy("id").sum("value1").collect()
end_disk = time.time()

# Cache in-memory
df.cache()
start_mem = time.time()
result_mem = df.groupBy("id").sum("value1").collect()
end_mem = time.time()

print(f"Disk-based processing time: {end_disk - start_disk:.4f} seconds")
print(f"In-memory processing time: {end_mem - start_mem:.4f} seconds")

