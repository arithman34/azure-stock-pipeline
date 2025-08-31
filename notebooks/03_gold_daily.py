# Databricks notebook source
import json

# Get parameters
dbutils.widgets.text("bronze_params", "")
dbutils.widgets.text("silver_params", "")

bronze_params = dbutils.widgets.get("bronze_params")
silver_params = dbutils.widgets.get("silver_params")

# Parse the JSON string
bronze_data = json.loads(bronze_params)

# Access the parameters
symbol = bronze_data.get("symbol", "")
yesterday = bronze_data.get("yesterday", "")
silver_adls = bronze_data.get("silver_adls", "")
gold_adls = bronze_data.get("gold_adls", "")
silver_path = silver_params

print(f"Parameters: symbol={symbol}, yesterday={yesterday}, silver_path={silver_path}, silver_adls={silver_adls}, gold_adls={gold_adls}")

# COMMAND ----------

if silver_path == "No bronze data for yesterday":
    print(f"No silver data for {yesterday} (likely weekend/holiday). Skipping gold load.")
    dbutils.notebook.exit("No silver data for yesterday")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, stddev_samp, lag, log, when, row_number

# COMMAND ----------

df_silver_all = (
    spark.read.format("delta").load(silver_path)
    .filter(col("date") <= yesterday)  # only past trading days
)

# COMMAND ----------

# Rank rows by date descending
w_desc = Window.partitionBy("symbol").orderBy(col("date").desc())
df_ranked = df_silver_all.withColumn("row_num", row_number().over(w_desc))

# COMMAND ----------

# Keep the last 31 trading rows
df_silver_window = df_ranked.filter(col("row_num") <= 31).drop("row_num")

# COMMAND ----------

df_silver_window.head()

# COMMAND ----------

w7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)   # last 7 days
w30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0) # last 30 days
w1 = Window.partitionBy("symbol").orderBy("date")

# COMMAND ----------

df_gold_window  = (
    df_silver_window
    .withColumn("SMA_7", avg("close").over(w7))  # simple moving average
    .withColumn("SMA_30", avg("close").over(w30))
    .withColumn("return", (col("close") - lag("close").over(w1)) / lag("close").over(w1))
    .withColumn("log_return", log(col("close") / lag("close").over(w1)))
    .withColumn("vol_7", stddev_samp("return").over(w7))  # volatility
    .withColumn("vol_30", stddev_samp("return").over(w30))
    .withColumn("zscore_return", (col("return") - avg("return").over(w30)) / stddev_samp("return").over(w30))
)

# COMMAND ----------

df_gold_yesterday = df_gold_window.filter(col("date") == yesterday)  # Get only yesterday

# COMMAND ----------

df_gold_yesterday.head()

# COMMAND ----------

if df_gold_yesterday.count() == 0:
    print(f"No gold data for {yesterday} (likely weekend/holiday).")

else:
    output_path = f"{gold_adls}/{symbol.lower()}/"

    (df_gold_yesterday
        .write
        .mode("append")
        .format("delta")
        .save(output_path)
    )

    print(f"Gold incremental row for {yesterday} written to {output_path}")