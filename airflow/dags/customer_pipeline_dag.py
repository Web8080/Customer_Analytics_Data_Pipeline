# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# import pandas as pd
# import os

# # Paths (update if needed)
# RAW_DATA_PATH = "/Users/user/airflow/data/raw/customers.csv"
# TRANSFORMED_DATA_PATH = "/Users/user/airflow/data/processed/customers_transformed.csv"

# # Functions for pipeline tasks
# def extract_customers():
#     """Extract raw customer data."""
#     os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
#     # Example: simulate extraction by creating a CSV
#     df = pd.DataFrame({
#         "customer_id": [1, 2, 3],
#         "first_name": ["Alice", "Bob", "Charlie"],
#         "last_name": ["Smith", "Jones", "Brown"],
#         "email": ["alice@email.com", "bob@email.com", "charlie@email.com"],
#         "signup_date": ["2025-01-01", "2025-02-01", "2025-03-01"]
#     })
#     df.to_csv(RAW_DATA_PATH, index=False)
#     print("Extracted raw customer data.")

# def transform_customers():
#     """Transform customer data."""
#     df = pd.read_csv(RAW_DATA_PATH)
#     # Example transformation: create full_name column
#     df["full_name"] = df["first_name"] + " " + df["last_name"]
#     os.makedirs(os.path.dirname(TRANSFORMED_DATA_PATH), exist_ok=True)
#     df.to_csv(TRANSFORMED_DATA_PATH, index=False)
#     print("Transformed customer data.")

# def load_customers():
#     """Load transformed data to target (simulated here)."""
#     df = pd.read_csv(TRANSFORMED_DATA_PATH)
#     print("Loading customers to target system:")
#     print(df.head())

# # DAG definition
# with DAG(
#     dag_id="customer_pipeline",
#     schedule="@daily",  # <- updated keyword
#     ...
# ) as dag:


#     task_extract = PythonOperator(
#         task_id="extract_customers",
#         python_callable=extract_customers
#     )

#     task_transform = PythonOperator(
#         task_id="transform_customers",
#         python_callable=transform_customers
#     )

#     task_load = PythonOperator(
#         task_id="load_customers",
#         python_callable=load_customers
#     )

#     # Set task dependencies
#     task_extract >> task_transform >> task_load



from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from snowflake.connector import connect

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "user": "XTAINLESSTECH",
    "password": "jynnaqWowpit2xazqe",
    "account": "BFZPUDT-WO35181",
    "warehouse": "MY_WH",
    "database": "MY_PROJECT",
    "schema": "RAW"
}

TRANSFORMED_DATA_PATH = "/Users/user/airflow/data/processed/customers_transformed.csv"

# Functions
def extract_customers():
    """Extract customer data from Snowflake"""
    conn = connect(**SNOWFLAKE_CONFIG)
    query = "SELECT * FROM raw_customers"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Save raw data locally for transformation
    df.to_csv("/Users/user/airflow/data/raw/customers.csv", index=False)
    print("Extracted customer data from Snowflake.")

def transform_customers():
    """Transform data"""
    df = pd.read_csv("/Users/user/airflow/data/raw/customers.csv")
    df["full_name"] = df["FIRST_NAME"] + " " + df["LAST_NAME"]
    df.to_csv(TRANSFORMED_DATA_PATH, index=False)
    print("Transformed customer data.")

def load_customers():
    """Load transformed data (simulate loading)"""
    df = pd.read_csv(TRANSFORMED_DATA_PATH)
    print("Loading transformed customers:")
    print(df.head())
    # Here you can insert code to push back to Snowflake ANALYTICS schema if needed

# DAG definition
with DAG(
    dag_id="customer_pipeline_snowflake",
    start_date=datetime(2025, 9, 5),
    schedule="@daily",
    catchup=False,
    tags=["snowflake", "customer_pipeline"]
) as dag:

    task_extract = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers
    )

    task_transform = PythonOperator(
        task_id="transform_customers",
        python_callable=transform_customers
    )

    task_load = PythonOperator(
        task_id="load_customers",
        python_callable=load_customers
    )

    task_extract >> task_transform >> task_load



import pandas as pd
import snowflake.connector
from scipy import stats
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ----------------------------
# Advanced Data Quality Check
# ----------------------------
def advanced_quality_check():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user="XTAINLESSTECH",
        password="jynnaqWowpit2xazqe",
        account="BFZPUDT-WO35181",
        warehouse="MY_WH",
        database="MY_PROJECT",
        schema="ANALYTICS"
    )

    # Load tables
    fact_orders = pd.read_sql("SELECT * FROM FACT_ORDERS", conn)
    customers = pd.read_sql("SELECT customer_id FROM DIM_CUSTOMERS", conn)
    products = pd.read_sql("SELECT product_id FROM DIM_PRODUCTS", conn)

    issues_log = []

    # --- Anomaly Detection (quantity, price) ---
    for col in ['quantity', 'price']:
        if col in fact_orders.columns:
            z_scores = stats.zscore(fact_orders[col].astype(float))
            anomalies = fact_orders[abs(z_scores) > 3]
            for _, row in anomalies.iterrows():
                issue_msg = (
                    f"Flagged Order ID: {row['order_id']} "
                    f"for unusual {col}={row[col]}. "
                    f"Suggestion: Compare with average {col} across products."
                )
                print(issue_msg)
                issues_log.append(issue_msg)

                # --- Simple Imputation Example ---
                # Replace with median value (or leave for review)
                median_val = fact_orders[col].median()
                fact_orders.loc[row.name, col] = median_val

    # --- Referential Integrity Checks ---
    missing_customers = fact_orders[~fact_orders['customer_id'].isin(customers['customer_id'])]
    missing_products = fact_orders[~fact_orders['product_id'].isin(products['product_id'])]

    for _, row in missing_customers.iterrows():
        issue_msg = (
            f"Order ID: {row['order_id']} has invalid customer_id={row['customer_id']}. "
            f"Suggestion: Check customer table or assign to 'Unknown Customer'."
        )
        print(issue_msg)
        issues_log.append(issue_msg)
        # Impute with placeholder
        fact_orders.loc[row.name, 'customer_id'] = -1

    for _, row in missing_products.iterrows():
        issue_msg = (
            f"Order ID: {row['order_id']} has invalid product_id={row['product_id']}. "
            f"Suggestion: Validate product mapping or assign to 'Unknown Product'."
        )
        print(issue_msg)
        issues_log.append(issue_msg)
        # Impute with placeholder
        fact_orders.loc[row.name, 'product_id'] = -1

    # --- Write back cleaned data to a staging table ---
    fact_orders.to_sql("FACT_ORDERS_CLEAN", conn, if_exists="replace", index=False)

    # --- Optionally log issues in Snowflake ---
    if issues_log:
        issues_df = pd.DataFrame({"issue_description": issues_log})
        issues_df.to_sql("DATA_QUALITY_ISSUES", conn, if_exists="append", index=False)

    conn.close()


# ----------------------------
# DAG Definition
# ----------------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 1),
    'retries': 1,
}

with DAG(
    dag_id="customer_pipeline_snowflake",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Existing tasks (extract, transform, load, dbt, etc.)
    # ...

    # New Advanced Quality Check task
    quality_check_task = PythonOperator(
        task_id='advanced_quality_check',
        python_callable=advanced_quality_check,
    )

    # Example dependencies
    # dbt_transform_task >> quality_check_task >> analytics_reporting_task
