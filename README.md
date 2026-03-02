Telco Customer Churn – Phase 1
Data Engineering & ETL Foundation
Project Overview

This repository contains Phase 1 of a Customer Churn Analytics project.

The focus of this phase is to establish a solid data engineering pipeline before moving into advanced analytics and machine learning.

What Has Been Implemented
1️⃣ Data Preprocessing (Python)

Raw Excel dataset ingestion

Structural data inspection (shape, types, missing values, uniqueness)

Removal of irrelevant columns

Data type correction (e.g., Total Charges → numeric)

Missing value handling

Clean dataset ready for database storage

Notebook:

notebooks/01_preprocessing_and_sql_loading.ipynb
2️⃣ SQL Server Integration

Cleaned dataset loaded into SQL Server

Table: telco_churn_data

No normalization or star schema applied yet

Current design is a flat analytical table

Database Name:

churn_analysis_project
3️⃣ Airflow Monthly ETL Pipeline

Implemented a production-style ETL simulation:

DAG ID:

telco_monthly_pipeline

Pipeline Flow:

Extract (Simulated Source Data Generation)
→ Preprocessing
→ Load to SQL Server

Features:

Monthly schedule

Automatic random data generation

Type casting & validation

Append to SQL table

Retry logic

Location:

airflow/telco_monthly_pipeline.py
4️⃣ Power BI Dashboard (Initial Prototype)

Connected directly to SQL Server.

Includes:

Total Revenue

Total Customers

Churned Customers

Churn Rate %

Contract Type Analysis

Tenure Analysis

Payment Method Distribution

Risk Segmentation

⚠️ This dashboard is an initial analytical prototype and will be enhanced in future phases.

Current Architecture

Simulated Source
→ Python Preprocessing
→ SQL Server
→ Airflow Automation
→ Power BI Dashboard

Technologies Used

Python (Pandas)

SQL Server

SQLAlchemy

PyODBC

Apache Airflow

Power BI

Git & GitHub

What Is NOT Implemented Yet

Data Modeling (Star Schema)

Denormalization Strategy

Feature Engineering

Machine Learning

Model Evaluation

Deployment

Retention Optimization Engine

These will be implemented in Phase 2 & 3.

Project Status

🚧 Active Development – Phase 1 Complete
Next step: Data Modeling & ML Layer
