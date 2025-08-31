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

import requests
import pandas as pd
from pyspark.sql.functions import year, month

# COMMAND ----------

dbutils.widgets.text("symbol", "AAPL")
dbutils.widgets.text("yesterday", "")

symbol = dbutils.widgets.get("symbol")
yesterday = dbutils.widgets.get("yesterday")

# COMMAND ----------

API_KEY = dbutils.secrets.get("apple-anomaly-dev", "api-key")

# API endpoint
url = (
    f"https://www.alphavantage.co/query?"
    f"function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=compact&apikey={API_KEY}"
)

try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if not data:
        print("No data received from the API.")
    else:
        ts = data.get("Time Series (Daily)", {})

        # Get yesterday's candle if available
        yesterdays_candle = ts.get(yesterday, None)

        if yesterdays_candle:
            # Convert to pandas DataFrame
            df = pd.DataFrame([{
                "date": yesterday,
                "open": yesterdays_candle["1. open"],
                "high": yesterdays_candle["2. high"],
                "low": yesterdays_candle["3. low"],
                "close": yesterdays_candle["4. close"],
                "volume": yesterdays_candle["5. volume"],
                "symbol": symbol
            }])

            # Convert to Spark DataFrame
            df_spark = spark.createDataFrame(df)

            # Add partitioning
            df_spark = df_spark.withColumn("year", year("date")).withColumn("month", month("date"))

            # Save to bronze
            output_path = f"{bronze_adls}/{symbol.lower()}/"
            (df_spark
                .write
                .mode("append")   # incremental
                .format("delta")
                .partitionBy("year", "month")
                .save(output_path)
            )

            print(f"Bronze incremental row for {yesterday} written to {output_path}")
        else:
            print(f"No data available for {yesterday} (likely weekend/holiday)")

except requests.exceptions.RequestException as e:
    print(f"Error: {e}")


# COMMAND ----------

import json

# COMMAND ----------

output_data = {
    "yesterday": yesterday,
    "symbol": symbol,
    "bronze_adls": bronze_adls,
    "silver_adls": silver_adls,
    "gold_adls": gold_adls
}

# Serialize the data to JSON
output_json = json.dumps(output_data)

# Return the JSON string
dbutils.notebook.exit(output_json)