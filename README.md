<div align="center">

# 📡 Telco Customer Churn Analytics
### End-to-End Data Engineering & Machine Learning Project



</div>

---

## 🎯 Project Overview

A production-style, end-to-end data analytics project built on a **Telco Customer Churn** dataset of **7,000+ customers**. The project covers the full data lifecycle — from raw ingestion to an interactive ML-powered prediction app.

> **Goal:** Identify customers likely to churn and enable data-driven retention strategies.

---

## 🗂️ Project Structure

```
DEPI-Graduation-Project/
│
├── 📁 data/                          # Raw & cleaned datasets
│   └── Telco_customer_churn.xlsx
│
├── 📁 notebook/                      # Jupyter notebooks
│   ├── preprossing_and_loading_to_database_.ipynb
│   └── ML_Final.ipynb
│
├── 📁 ml_app/                        # Streamlit prediction app
│   ├── app.py
│   ├── requirements.txt
│   ├── save_model.py
│   ├── lgbm_model.pkl
│   └── feature_cols.pkl
│
├── 📁 dashboard/                     # Power BI dashboard
│   └── CustomerChurn.pbix
│
├── 📁 airflow_part/                  # Airflow ETL pipeline
│   └── telco_monthly_pipeline.py
│
└── 📄 README.md
```

---

## 🏗️ Architecture

```
Raw Excel Data
      │
      ▼
┌─────────────────┐
│  Python         │  preprossing_and_loading_to_database_.ipynb
│  Preprocessing  │  • Type correction  • Missing value handling
└────────┬────────┘  • Column removal   • Data validation
         │
         ▼
┌─────────────────┐
│  SQL Server     │  Database: churn_analysis_warhouse
│  Star Schema    │  ┌──────────────────────────────────┐
│                 │  │  fact_customer_churn              │
│                 │  │  dim_customer  dim_services       │
│                 │  │  dim_payment   dim_contract       │
└────────┬────────┘  └──────────────────────────────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌───────┐  ┌──────────┐
│SSIS   │  │ML Model  │  ML_Final.ipynb
│ETL    │  │LightGBM  │  • EDA  • Feature Engineering
│       │  │          │  • Decision Tree → LightGBM
└───────┘  └────┬─────┘
                │
           ┌────┴────┐
           ▼         ▼
    ┌──────────┐  ┌──────────┐
    │Streamlit │  │ Power BI │
    │   App    │  │Dashboard │
    └──────────┘  └──────────┘
```

---

## ✅ What's Implemented

### 1️⃣ Data Preprocessing — `notebook/preprossing_and_loading_to_database_.ipynb`

| Step | Detail |
|------|--------|
| Ingestion | Raw Excel file (`Telco_customer_churn.xlsx`) |
| Inspection | Shape, dtypes, missing values, uniqueness |
| Cleaning | Dropped irrelevant columns (Zip Code, Lat/Long, Country, State…) |
| Type Fix | `Total Charges` → numeric (was string) |
| Missing Values | Dropped rows where `Total Charges` is null |
| Output | Clean dataset loaded into SQL Server (`telco_churn_data`) |

---

### 2️⃣ Data Warehouse — Star Schema (SQL Server)

```
                    ┌──────────────────┐
                    │  dim_customer    │
                    │  gender          │
                    │  senior_citizen  │
                    │  partner         │
                    │  dependents      │
                    └────────┬─────────┘
                             │
┌──────────────┐    ┌────────┴──────────┐    ┌──────────────────┐
│ dim_services │────│ fact_customer_    │────│  dim_payment     │
│ internet     │    │ churn             │    │  payment_method  │
│ tech_support │    │ tenure_months     │    │  paperless_bill  │
│ online_sec   │    │ monthly_charges   │    └──────────────────┘
└──────────────┘    │ cltv              │
                    │ churn_label       │    ┌──────────────────┐
                    │ churn_score       │────│  dim_contract    │
                    └───────────────────┘    │  contract_type   │
                                             └──────────────────┘
```

**Database:** `churn_analysis_warhouse`

---

### 3️⃣ SSIS ETL Pipeline — `airflow_part/`

Visual ETL pipeline built with **SQL Server Integration Services (SSIS)**:

```
Sequence Container
├── Load_dim_customer
├── load_dim_contract
├── load_dim_services
└── load_dim_payment
        │
        ▼
   fact_table  (Data Flow Task)

Source: OLTP_Source_DB  →  Destination: OLAP_Destination_warhouse
```

---

### 4️⃣ Machine Learning — `notebook/ML_Final.ipynb`

**Data source:** SQL Server warehouse via SQLAlchemy JOIN across all dim tables

#### Preprocessing
| Step | Detail |
|------|--------|
| Dropped | PK/FK keys, `churn_score` (data leakage), `city` (high cardinality) |
| Dropped | `total_charges` (0.84 correlation with `tenure_months`) |
| Binary Encoding | `partner`, `dependents`, `paperless_billing`, `senior_citizen`, `gender` |
| One-Hot Encoding | `internet_service`, `tech_support`, `online_security`, `payment_method`, `contract_type` |

#### Split Strategy
```
Full Dataset  (100%)
├── Train     (70%)
├── Validation(15%)
└── Test      (15%)   [stratified on churn_label]
```

#### Models

| Model | Purpose | Notes |
|-------|---------|-------|
| Decision Tree | Baseline + Feature Selection | threshold > 0.1 importance |
| **LightGBM** | **Final Model** | 500 estimators, lr=0.03, max_depth=4 |

#### LightGBM Configuration
```python
LGBMClassifier(
    n_estimators   = 500,
    learning_rate  = 0.03,
    max_depth      = 4,
    num_leaves     = 20,
    subsample      = 0.8,
    colsample_bytree = 0.8,
    random_state   = 42
)
```

#### Top Predictive Features
```
1. contract_type        ████████████████████  (strongest)
2. tenure_months        ████████████████
3. online_security      █████████████
4. tech_support         ████████████
5. monthly_charges      ██████████
6. payment_method       ████████
7. cltv                 ███████
```

#### Key Insight
> Customers on **month-to-month contracts** with **no tech support** or **online security**, paying via **electronic check**, and with **low tenure** are the highest churn risk.

---

### 5️⃣ Power BI Dashboard — `dashboard/`

<table>
<tr>
<td><b>Total Revenue</b><br>16.08M</td>
<td><b>Total Customers</b><br>7K</td>
<td><b>Churned Customers</b><br>2K</td>
<td><b>Churn Rate</b><br>27%</td>
<td><b>High Risk</b><br>1K</td>
</tr>
</table>

**Visuals:**
- Churned Customers by Contract Type (Month-to-month: 1,657 vs One year: 170 vs Two year: 49)
- Customer Churn by Tenure Group
- Payment Methods of Churned Customers (Electronic check: 57%)
- Risk Segmentation (Low / Medium / High / Very High)

---

### 6️⃣ Streamlit Prediction App — `ml_app/`

Live churn prediction using the trained LightGBM model.

**Features:**
- 🎲 Random Fill button for instant testing
- ⚡ Real-time prediction with probability score
- 📊 Visual risk gauge
- 🔴🟢 Risk factor chips (what's driving the prediction)

**Run locally:**
```bash
cd ml_app
pip install -r requirements.txt
streamlit run app.py
```

> **Note:** Generate model files first by running the last cell in `ML_Final.ipynb`:
> ```python
> import joblib
> joblib.dump(lgbm, 'lgbm_model.pkl')
> joblib.dump(list(x_train.columns), 'feature_cols.pkl')
> ```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Data Processing | Pandas, NumPy |
| ML | Scikit-learn, LightGBM |
| Visualization (EDA) | Plotly Express |
| Database | SQL Server 2019 |
| ORM | SQLAlchemy + PyODBC |
| ETL | SSIS (SQL Server Integration Services) |
| BI Dashboard | Power BI |
| Web App | Streamlit |
| Version Control | Git & GitHub |

---

## 👥 Team

**DEPI Graduation Project — 2025**

---

<div align="center">
<i>Built with ❤️ as part of the Digital Egypt Pioneers Initiative (DEPI)</i>
</div>
