# Azure_ETL_ADF_Project
End To End Azure Project.
# Azure ETL ADF Project 🚀

## 📌 Project Overview
This project demonstrates a complete **ETL data pipeline** using **Azure Data Factory (ADF), Databricks, and Azure SQL Database**.  
It extracts raw data from source systems, stages it in Azure SQL DB, applies **SCD1, SCD2, and pass-through transformations**, and loads curated tables into the **Integration Layer** for analytics and reporting.

The project is scheduled to run **daily at 9 AM** and includes sample Power BI dashboards for insights.

---

## 🏗️ Architecture
**Layers:**
1. **Ingestion Layer** – Raw data loaded from source into staging (`stg.*`) tables via Databricks.
2. **Integration Layer** – Data transformations applied:
   - **SCD1**: Overwrites old records (latest state).
   - **SCD2**: Preserves history with `BEGIN_DATE`, `END_DATE`, `ACTIVE_FLAG`.
   - **Pass-through**: Direct fields without transformation.
3. **Presentation Layer** – Curated dimension and fact tables for reporting (Power BI).

**Tech Stack:**
- **Azure Data Factory (ADF)** → Orchestration, pipeline scheduling
- **Azure SQL Database** → Staging + Integration layer
- **Azure Databricks** → Data transformation, validation
- **GitHub** → Version control for code, scripts, pipelines
- **Power BI** → Reporting & dashboards

---

## 📂 Repository Structure
AZURE_ETL_PROJECT/
├── DATA_SAMPLES/ # Example CSVs (non-confidential sample data)
├── SQL_SCRIPT/ # SQL DDL scripts + Stored Procedures
├── NOTEBOOKS/ # Databricks notebooks (Py/Notebook format)
├── DOCS/ # Documentation (Report + Presentation)
│ ├── ETL_Project_Report.docx
│ ├── ETL_Project_Presentation.pptx
├── PIPELINES/ # Exported JSON of ADF pipelines
├── README.md

---

## ⚙️ How It Works
1. **Ingestion:**
   - Source data extracted daily from transactional DBs → loaded into **staging** tables via Databricks/JDBC.
2. **Integration:**
   - SQL merge logic applied to manage **SCD1, SCD2, and pass-through columns**.
   - Surrogate keys generated.
3. **Scheduling:**
   - ADF pipeline triggers orchestration at **09:00 AM daily**.
4. **Validation:**
   - Databricks notebooks validate row counts, null checks, and business rules.
5. **Reporting:**
   - Data consumed in **Power BI dashboards** for KPIs such as YoY Growth, MoM Growth, etc.

---
