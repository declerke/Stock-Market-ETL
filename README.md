# 📈 QuantLake: Cloud-Native Financial Intelligence & Metadata Sanitization Engine

**QuantLake** is a professional-grade Data Engineering pipeline designed to bridge the gap between messy API-sourced financial data and executive-level investment intelligence. It features a hardened Google Cloud stack that navigates the complex challenges of "Schema Poisoning" in Parquet metadata to deliver high-fidelity stock analysis through Looker Studio.

---

## 🎯 Project Goal
To engineer a resilient, end-to-end ETL pipeline that extracts US stock market fundamentals, overcomes strict BigQuery ingestion constraints via custom metadata sanitization, and executes multi-dimensional analysis on corporate efficiency (ROE/ROA) and solvency.

---

## 🧬 System Architecture
The pipeline is built on a modular **"Ingest-Sanitize-Analyze"** logic:

1. **Orchestrated Ingestion:** Automated extraction from **SimFin API** to **GCS Raw** (Parquet) using **Prefect Cloud**.
2. **Distributed Transformation:** Leveraging **Apache Spark 3.5.0** to calculate complex financial ratios (Profit Margin, ROE, ROA, Debt-to-Equity).
3. **Metadata Sanitization Bridge:** A custom-built Python layer designed to bypass BigQuery's Parquet reader limitations by "washing" illegal characters (`.`, `()`) from headers.

4. **Warehouse Layer:** Optimized **BigQuery** native tables, partitioned by `Year` to ensure high-performance, cost-effective analytical serving.
5. **Intelligence Layer:** Executive-grade **Looker Studio** dashboards for ticker-level quantitative screening.

---

## 🛠️ Technical Stack
| Layer | Tools | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | Prefect Cloud | Workflow scheduling, state management, and failure monitoring |
| **Data Lake** | GCS (Google Cloud Storage) | Scalable landing zone for Raw and Transformed Parquet data |
| **Data Processing** | Apache Spark, Pandas | Distributed heavy-lifting and metadata sanitization |
| **Infrastructure** | Terraform | Infrastructure as Code (IaC) for GCP resource provisioning |
| **Data Warehouse** | Google BigQuery | High-performance SQL engine with Hive Partitioning support |
| **Business Intelligence** | Looker Studio | Interactive dashboards for quantitative stock analysis |

---

## 📊 Performance & Results
* **Metadata Resilience:** Resolved a "Critical Ingestion Failure" (Character '.' not supported) by implementing a custom CSV sanitization bridge.
* **Storage Optimization:** Leveraged **Hive Partitioning** by `Year`, reducing BigQuery query costs by ensuring targeted partition pruning.
* **Data Integrity:** Employed a **Partial Schema Load** strategy to filter out 40+ secondary columns, focusing only on high-signal financial metrics.
* **Quant Insights:** Developed an automated scoring system based on **ROE** and **Current Ratio** to identify top-performing tickers in the 2024-2026 dataset.

---

## 📂 Project Structure
```text
QuantLake/
├── terraform/              # IaC: Provisioning GCS buckets & BigQuery datasets
├── flows/                  # Prefect flows: extract.py, transform.py, load.py
├── spark/                  # PySpark logic: Financial ratio calculations
├── data_wash/              # Metadata Sanitization: sanitize_data.py (Pandas)
├── schemas/                # JSON schema definitions for BigQuery enforcement
├── requirements.txt        # Python dependency list
└── README.md
```

---

## ⚙️ Installation & Setup

### 1. Infrastructure Provisioning
Initialize and apply the Terraform configuration to set up your Google Cloud environment.
```bash
cd terraform
terraform init
terraform apply
```

### 2. Run the ETL Pipeline
Execute the orchestrated flow to extract and transform the data.
```bash
# Authenticate with Prefect
prefect cloud login

# Run the end-to-end orchestration
python flows/orchestrate.py
```

### 3. Metadata Sanitization (The "Bypass")
If BigQuery rejects the Parquet headers, run the sanitization script to generate clean CSVs.
```bash
python data_wash/sanitize_data.py
```

---

## 🎓 Skills Demonstrated
* **Hybrid Processing:** Balancing **Spark** for big data transformations with **Pandas** for localized metadata cleaning.
* **Cloud Infrastructure:** Architecting a multi-service GCP environment using **Terraform**.
* **Problem Solving:** Navigating the "Last Mile" ingestion gap between disparate metadata standards (Spark Parquet vs. BigQuery).
* **Business Intelligence:** Crafting robust dashboards that transform raw ratios into **Solvency** and **Efficiency** indicators.

---

### Future Enhancements
* Implement **dbt** for SQL-based modeling inside BigQuery.
* Add **Great Expectations** for automated data quality audits during the Spark phase.
* Integrate **GitHub Actions** for CI/CD of Terraform and Prefect deployments.
