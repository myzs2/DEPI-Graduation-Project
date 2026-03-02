from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import random
import string
import os
from sqlalchemy import create_engine

# =========================
# CONFIG
# =========================

SERVER = "host.docker.internal,1433"
DATABASE = "churn_analysis_project"
USERNAME = "sa"
PASSWORD = "1234"
TABLE_NAME = "telco_churn_data"

# =========================
# DEFAULT ARGS
# =========================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# =========================
# DAG
# =========================

with DAG(
    dag_id="telco_monthly_pipeline",
    default_args=default_args,
    description="Monthly Telco ETL Pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["telco", "etl", "monthly"]
) as dag:

    # =====================================
    # 1️⃣ EXTRACT (Generate as Source)
    # =====================================

    def extract_from_source(**context):

        execution_date = context["ds"].replace("-", "_")
        rows_count = random.randint(5, 15)

        def generate_customer_id():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

        data = []

        for _ in range(rows_count):
            tenure = random.randint(1, 72)
            monthly = round(random.uniform(20, 120), 2)

            data.append({
                "CustomerID": generate_customer_id(),
                "City": random.choice(["Los Angeles","San Diego","San Jose","Sacramento","Fresno"]),
                "Gender": random.choice(["Male","Female"]),
                "Senior Citizen": random.choice(["Yes","No"]),
                "Partner": random.choice(["Yes","No"]),
                "Dependents": random.choice(["Yes","No"]),
                "Tenure Months": tenure,
                "Phone Service": random.choice(["Yes","No"]),
                "Multiple Lines": random.choice(["Yes","No","No phone service"]),
                "Internet Service": random.choice(["DSL","Fiber optic","No"]),
                "Online Security": random.choice(["Yes","No","No internet service"]),
                "Online Backup": random.choice(["Yes","No","No internet service"]),
                "Device Protection": random.choice(["Yes","No","No internet service"]),
                "Tech Support": random.choice(["Yes","No","No internet service"]),
                "Streaming TV": random.choice(["Yes","No","No internet service"]),
                "Streaming Movies": random.choice(["Yes","No","No internet service"]),
                "Contract": random.choice(["Month-to-month","One year","Two year"]),
                "Paperless Billing": random.choice(["Yes","No"]),
                "Payment Method": random.choice([
                    "Electronic check",
                    "Mailed check",
                    "Bank transfer (automatic)",
                    "Credit card (automatic)"
                ]),
                "Monthly Charges": monthly,
                "Total Charges": round(tenure * monthly, 2),
                "Churn Label": random.choice(["Yes","No"]),
                "Churn Score": random.randint(0, 100),
                "CLTV": random.randint(2000, 6000),
                "Churn Reason": random.choice([
                    "Competitor made better offer",
                    "Moved",
                    "Competitor had better devices",
                    "Competitor offered higher download speeds",
                    "Competitor offered more data"
                ])
            })

        df = pd.DataFrame(data)

        file_path = f"/opt/airflow/telco_{execution_date}.csv"
        df.to_csv(file_path, index=False)

        return file_path


    # =====================================
    # 2️⃣ PREPROCESSING
    # =====================================

    def pre_processing(**context):

        ti = context["ti"]
        file_path = ti.xcom_pull(task_ids="extract_from_source")

        df = pd.read_csv(file_path)

        numeric_cols = ["Tenure Months", "Monthly Charges", "Total Charges", "Churn Score", "CLTV"]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df.dropna(subset=["Total Charges"], inplace=True)

        cleaned_path = file_path.replace(".csv", "_cleaned.csv")
        df.to_csv(cleaned_path, index=False)

        return cleaned_path


    # =====================================
    # 3️⃣ LOAD TO SQL
    # =====================================

    def load_to_sql(**context):

        ti = context["ti"]
        cleaned_path = ti.xcom_pull(task_ids="pre_processing")

        df = pd.read_csv(cleaned_path)

        engine = create_engine(
            f"mssql+pyodbc://{USERNAME}:{PASSWORD}"
            f"@{SERVER}/{DATABASE}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )

        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)

        return f"{len(df)} rows loaded successfully."


    # =====================================
    # TASK DEFINITIONS
    # =====================================

    task_extract = PythonOperator(
        task_id="extract_from_source",
        python_callable=extract_from_source
    )

    task_preprocess = PythonOperator(
        task_id="pre_processing",
        python_callable=pre_processing
    )

    task_load = PythonOperator(
        task_id="load_to_sql",
        python_callable=load_to_sql
    )

    # DAG FLOW
    task_extract >> task_preprocess >> task_load