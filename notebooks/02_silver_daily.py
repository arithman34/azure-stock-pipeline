# Databricks notebook source
import json

# Get the parameters from bronze
bronze_params = dbutils.widgets.get("bronze_params")
bronze_data = json.loads(bronze_params)

# Access the parameters
symbol = bronze_data.get("symbol", "AAPL")
yesterday = bronze_data.get("yesterday", "")
bronze_adls = bronze_data.get("bronze_adls", "")
silver_adls = bronze_data.get("silver_adls", "")
gold_adls = bronze_data.get("gold_adls", "")

print(f"Parameters: symbol={symbol}, yesterday={yesterday}, bronze_adls={bronze_adls}, silver_adls={silver_adls}, gold_adls={gold_adls}")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

bronze_path = f"{bronze_adls}/{symbol.lower()}/"
df_bronze_new = spark.read.format("delta").load(bronze_path).filter(col("date") == yesterday)

# COMMAND ----------

if df_bronze_new.count() == 0:
    print(f"No bronze data for {yesterday} (likely weekend/holiday). Skipping silver load.")
    dbutils.notebook.exit("No bronze data for yesterday")

# COMMAND ----------

df_silver = (
    df_bronze_new
    .select(
        col("date"),
        col("symbol"),
        col("open").cast("double"),
        col("high").cast("double"),
        col("low").cast("double"),
        col("close").cast("double"),
        col("volume").cast("long"),
        current_timestamp().alias("ingestion_time")
    )
)

# COMMAND ----------

silver_output_path = f"{silver_adls}/{symbol.lower()}/"
df_silver.write.mode("append").format("delta").save(silver_output_path)

print(f"Silver incremental row for {yesterday} written to {silver_output_path}")

dbutils.notebook.exit(silver_output_path)