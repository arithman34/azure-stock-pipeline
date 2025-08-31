# Databricks notebook source
# Mount ADLS Gen2
tiers = ["bronze", "silver", "gold"]
adls_paths = {tier: f"abfss://{tier}@stapplestockanomaly.dfs.core.windows.net/" for tier in tiers}

# Accessing paths
bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]

dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Backfill

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql.functions import year, month

# COMMAND ----------

API_KEY = dbutils.secrets.get("apple-anomaly-dev", "api-key")
symbol = "AAPL"

# API endpoint
url = (
    f"https://www.alphavantage.co/query?"
    f"function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={API_KEY}"
)

try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if not data:
        print("No data received from the API.")
    else:
        ts = data.get("Time Series (Daily)", {})

        # Convert to pandas DataFrame
        df = pd.DataFrame.from_dict(ts, orient="index")
        df = df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume"
        })

        df.index.name = "date"
        df.reset_index(inplace=True)
        df["symbol"] = symbol

        # Convert to Spark DataFrame
        df_spark = spark.createDataFrame(df)

        # Add partitioning
        df_spark = df_spark.withColumn("year", year("date")).withColumn("month", month("date"))

        # Save to bronze
        bronze_path = f"{bronze_adls}/{symbol.lower()}/"
        (df_spark
            .write
            .mode("overwrite")   # backfill
            .format("delta")
            .partitionBy("year", "month")
            .save(bronze_path)
        )

        print(f"Bronze backfill written to {bronze_path}")

except requests.exceptions.RequestException as e:
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Backfill

# COMMAND ----------

from pyspark.sql.functions import col, year, month, to_date

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

df_silver = (
    df_bronze
    .withColumn("date", to_date("date", "yyyy-MM-dd"))
    .select(
        col("date"),
        col("open").cast("double").alias("open"),
        col("high").cast("double").alias("high"),
        col("low").cast("double").alias("low"),
        col("close").cast("double").alias("close"),
        col("volume").cast("long").alias("volume"),
        col("symbol")
    )
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
)

# COMMAND ----------

df_silver.head()

# COMMAND ----------

silver_path = f"{silver_adls}/{symbol.lower()}/"

(df_silver
    .write
    .mode("overwrite")     # backfill
    .format("delta")
    .partitionBy("year", "month")
    .save(silver_path)
)

print(f"Silver backfill written to {silver_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold BackFill

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, stddev_samp, lag, log

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)
df_silver = df_silver.withColumn("date", to_date("date", "yyyy-MM-dd"))

# COMMAND ----------

df_silver.head()

# COMMAND ----------

# Define rolling windows
w7  = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
w30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)
w1  = Window.partitionBy("symbol").orderBy("date")

# COMMAND ----------

df_gold = (
    df_silver
    .withColumn("SMA_7", avg("close").over(w7))
    .withColumn("SMA_30", avg("close").over(w30))
    .withColumn("return", (col("close") - lag("close").over(w1)) / lag("close").over(w1))
    .withColumn("log_return", log(col("close") / lag("close").over(w1)))
    .withColumn("vol_7", stddev_samp("return").over(w7))
    .withColumn("vol_30", stddev_samp("return").over(w30))
    .withColumn("zscore_return", (col("return") - avg("return").over(w30)) / stddev_samp("return").over(w30))
)

# COMMAND ----------

df_gold.head(2)

# COMMAND ----------

df_gold.tail(1)

# COMMAND ----------

gold_path = f"{gold_adls}/{symbol.lower()}/"

(df_gold
    .write
    .mode("overwrite")       # backfill
    .format("delta")
    .save(gold_path)
)

print(f"Gold backfill written to {gold_path}")