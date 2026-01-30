# 🏥 MediPulse AI: Clinical Data Engineering & ML Pipeline

## 📋 1. Problem Definition
This project addresses the challenge of monitoring patient safety at scale. Using a dataset of **100,000 clinical encounters**, I built an automated system to identify senior patients at high risk for hospital readmission by correlating age demographics with abnormal vital signs (Heart Rate).

---
## 🏗️ 2. Data Architecture: Medallion
The project utilizes a **Medallion Architecture** on **Delta Lake** to ensure data quality and ACID transactions:
* **Bronze**: Automated ingestion of 100k raw healthcare records into permanent Delta storage.
* **Silver**: Schema enforcement and feature engineering, including the creation of the `age_group` column to segment Pediatrics, Adults, and Seniors.
* **Gold**: Production-ready tables filtering for high-risk seniors based on clinical business rules and risk classification.

---

## 🛠️ 3. Technical Implementation
### ⚙️ Orchestration & Workflow
* **Databricks Jobs**: The entire pipeline is orchestrated as a multi-task workflow. 
The `images/` folder contains the **Job Run** visualization, proving the automated success of all 3 stages.

### 🤖 ML Component & Governance
* **MLflow**: Managed the model lifecycle and experiment tracking, logging parameters for clinical risk thresholds.
* **Unity Catalog**: Implemented data governance and granular permissions to ensure clinical data privacy and lineage.
* **Transformations**: Utilized complex PySpark logic to clean noisy data and categorize patient severity levels.

---

## 📊 4. Analytics & Dashboard Insights
The **MediPulse Clinical Command Center** provides real-time decision support. For reproducibility, I have included the dashboard in **JSON** and **PDF** formats in the `images/` folder.
1. **Total Ingestion Volume**: Confirms the processing of 100,000 records.
2. **Demographic Distribution**: Visualizes the 31% Senior population targeted by this pipeline.
3. **Clinical Risk Correlation**: A scatter plot analyzing the intersection of Age vs. Heart Rate vs. Risk Status.
4. **Vital Averages**: Comparison of average heart rates across different demographic groups.

---

## 📁 5. Repository Structure
All project assets are consolidated into this single directory for full reproducibility:
* **`/notebooks`**: PySpark/MLflow source code (`01_bronze`, `02_silver`, `03_gold`).
* **`/sql_queries`**: SQL scripts for Unity Catalog setup, analytics, and data auditing.
* **`/images`**: Project assets including:
    * **Job Run Screenshot**: Proof of successful automated workflow.
    * **Dashboard (PDF)**: Professional report export.
    * **Dashboard (JSON)**: Technical export for dashboard migration.

---

## 🚀 6. Findings & Business Impact
* **Clinical Value**: The system proactively flags high-risk seniors, allowing ER triage teams to prioritize care for the most vulnerable 31% of the patient population.
* **Scalability**: Built on Delta Lake, the pipeline is optimized to handle increasing data volumes while maintaining sub-second query performance for the Command Center.
