# Azure Stock Pipeline

This project implements a **data engineering pipeline on Azure** for ingesting, transforming, and enriching stock market data using the **bronze–silver–gold lakehouse architecture**.  

It integrates **Azure Databricks** (data processing) with **Azure Data Factory (ADF)** (orchestration) and stores results in **Azure Data Lake Storage Gen2 (ADLS)**.  

The pipeline retrieves stock prices from the [Alpha Vantage API](https://www.alphavantage.co/) and prepares enriched features for anomaly detection and analytics.

---

## 📂 Repository Structure

```
infra/                       # Infrastructure setup
│── cluster_config.json       # Databricks cluster configuration
│── keyvault_secrets.md       # Secret references (Alpha Vantage API key)
│── storage_mounts.md         # ADLS mount points (bronze, silver, gold)

notebooks/                   # Databricks notebooks for pipeline
│── 00_backfill_bronze_silver_gold.py   # Full backfill for bronze -> silver -> gold
│── 01_bronze_daily.py                  # Incremental daily load into bronze
│── 02_silver_daily.py                  # Incremental daily transform into silver
│── 03_gold_daily.py                    # Incremental daily transform into gold

adf/                         # Azure Data Factory templates
│── adf-stock-anomaly-pipeline_ARMTemplateForFactory.json          # Factory definition
│── adf-stock-anomaly-pipeline_ARMTemplateParametersForFactory.json
│── ARMTemplateForFactory.json                                    # Linked services, pipeline, trigger
│── ARMTemplateParametersForFactory.json
│── ArmTemplate_0.json
│── ArmTemplate_master.json
│── ArmTemplateParameters_master.json
```

---

## 🏗️ Infrastructure

### Databricks Cluster
- Configured in `infra/cluster_config.json`  
- Single node cluster (`Standard_D4ds_v5`)  
- Spark version: **15.4.x-scala2.12**  
- Auto-termination: **30 minutes**  

### Secrets
- Defined in `infra/keyvault_secrets.md`  
- Stored in **Azure Key Vault**  
- Required:  
  - `api-key` -> Alpha Vantage API key  

### Storage Mounts
- Defined in `infra/storage_mounts.md`  
- ADLS Gen2 containers:  
  - `bronze`: `abfss://bronze@stapplestockanomaly.dfs.core.windows.net/`  
  - `silver`: `abfss://silver@stapplestockanomaly.dfs.core.windows.net/`  
  - `gold`: `abfss://gold@stapplestockanomaly.dfs.core.windows.net/`  

---

## 🔄 Pipeline Overview

### Bronze Layer
- Raw stock OHLCV data pulled from Alpha Vantage (`TIME_SERIES_DAILY`).  
- Partitioned by `year` and `month`, stored as **Delta Lake**.  
- Scripts:  
  - `00_backfill_bronze_silver_gold.py` -> full history load  
  - `01_bronze_daily.py` -> daily incremental load  

### Silver Layer
- Data cleaning & typing:  
  - Proper `date` format  
  - Cast prices to `double`, volume to `long`  
- Scripts:  
  - `00_backfill_bronze_silver_gold.py` -> full history load  
  - `02_silver_daily.py` -> daily incremental load  

### Gold Layer
- Feature engineering for anomaly detection:  
  - Moving averages (`SMA_7`, `SMA_30`)  
  - Returns & log returns  
  - Volatility (`vol_7`, `vol_30`)  
  - Return z-scores
- Scripts:  
  - `00_backfill_bronze_silver_gold.py` -> full history load  
  - `03_gold_daily.py` -> daily incremental load  

---

## ⚙️ Orchestration with ADF

ADF orchestrates the notebooks using ARM templates in `adf/`.

### Linked Services
- `AzureDatabricks1`: connection to Databricks workspace & cluster.  

### Pipeline (`pipeline1`)
- **Activities**:
  1. Run **Bronze Notebook**
  2. Run **Silver Notebook** (depends on bronze success)
  3. Run **Gold Notebook** (depends on silver success)  

- Parameters:
  - `symbol` (default `"AAPL"`)  
  - `yesterday` (calculated with `@formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-dd')`)  

### Trigger
- **Daily schedule trigger** runs the pipeline once per day (UTC).  

---

## 🔑 Requirements

- **Azure Databricks**  
- **Azure Data Lake Storage Gen2**  
- **Azure Data Factory**  
- **Delta Lake**  
- **Alpha Vantage API Key** (stored in Key Vault)  
- Python dependencies:  
  - `pandas`  
  - `requests`  
  - `pyspark`  

---

## 🚀 Usage

### Initial Backfill
Run manually in Databricks:  

```bash
notebooks/00_backfill_bronze_silver_gold.py
```

### Daily Incremental Loads
Automated via ADF pipeline (`adf/ARMTemplateForFactory.json`) with daily trigger.  

Or run manually in order:
1. `01_bronze_daily.py`
2. `02_silver_daily.py`
3. `03_gold_daily.py`

---

## 📊 Outputs

- **Bronze** -> Raw OHLCV data  
- **Silver** -> Cleaned + typed data  
- **Gold** -> Enriched features for anomaly detection  

---

## 🔭 Future Work

- Extend to multiple tickers.  
- Build anomaly detection models directly on the **gold layer**.  
- Automate deployment with CI/CD for ADF + Databricks.  
- Expand to intraday or alternative data sources.  

---

## 🧩 Extend & Contribute
- Contributions, PRs, and issue reports are welcome!

## 📫 Contact
Interested in collaborating or have questions? Reach out via GitHub issues or email.

- **Author**: Arif A. Othman
- **Email**: arithman34@hotmail.com
- **GitHub**: [arithman34](https://github.com/arithman34)
- **LinkedIn**: [arithman34](https://www.linkedin.com/in/arithman34/)

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.