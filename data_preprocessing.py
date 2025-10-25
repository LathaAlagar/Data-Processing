
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, stddev, count, isnan, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StandardScaler, VectorAssembler
import pyspark.sql.functions as F

# Initialize Spark
spark = SparkSession.builder \
    .appName("Data Preprocessing Challenge") \
    .getOrCreate()

# Load dataset
df = spark.read.csv("all_stocks.csv", header=True, inferSchema=True)

# ----- 1️⃣ Handle Missing Values -----
# Fill numeric columns with mean
numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, DoubleType)]
for c in numeric_cols:
    mean_val = df.select(mean(col(c))).first()[0]
    df = df.na.fill({c: mean_val})

# Fill string columns with mode
string_cols = [f.name for f in df.columns if f not in numeric_cols]
for c in string_cols:
    mode_val = df.groupBy(c).count().orderBy(F.desc("count")).first()[0]
    df = df.na.fill({c: mode_val})

# ----- 2️⃣ Fix Data Type Inconsistencies -----
# Example: convert numeric strings to DoubleType
for c in df.columns:
    df = df.withColumn(c, when(col(c).rlike("^[0-9.]+$"), col(c).cast(DoubleType())).otherwise(col(c)))

# ----- 3️⃣ Remove Duplicates -----
df = df.dropDuplicates()

# ----- 4️⃣ Normalization / Standardization -----
numeric_cols = [c for c in df.columns if df.select(c).schema.fields[0].dataType == DoubleType()]
assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features_vector")
df_vec = assembler.transform(df)

scaler = StandardScaler(inputCol="features_vector", outputCol="scaled_features", withMean=True, withStd=True)
scaler_model = scaler.fit(df_vec)
df_scaled = scaler_model.transform(df_vec)

# ----- 5️⃣ Feature Engineering -----
# Example: create a ratio feature
if len(numeric_cols) >= 2:
    df_scaled = df_scaled.withColumn("feature_ratio", col(numeric_cols[0]) / (col(numeric_cols[1]) + 1))

# Save output
df_scaled.write.csv("data/cleaned_dataset.csv", header=True, mode="overwrite")

spark.stop()
print("✅ Data preprocessing complete! Cleaned dataset saved in /data.")
