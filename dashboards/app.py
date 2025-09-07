# This component of the project implements an interactive Sales Analytics Dashboard using Streamlit, Plotly, Pandas, and Snowflake. It is designed to provide real-time insights from the transformed and curated data produced by the ETL and dbt pipelines.
# Workflow Overview:
# Snowflake Connection
# The dashboard establishes a secure connection to the Snowflake data warehouse, accessing the MY_PROJECT.ANALYTICS schema. Queries are executed against curated tables and views, such as TOP_CUSTOMERS, PRODUCT_CATEGORY_SALES, and FACT_ORDERS_CLEAN.
# Data Extraction
# Python functions handle query execution and return results as Pandas DataFrames, ensuring seamless integration between Snowflake and Streamlit.
# Data Cleaning and Transformation
# Minor preprocessing steps are applied to prepare data for visualization, including type conversions and the creation of revenue segments for customer segmentation.
# Visualizations
# Using Plotly, the dashboard provides interactive and insightful visualizations:
# Top Customers: Bar chart highlighting the top 10 customers by revenue.
# Product Category Performance: Pie chart showing revenue distribution across product categories.
# Customer Segmentation: Histogram grouping customers into Low, Medium, and High revenue tiers.
# Anomalies Flagged: Tabular view of high-value orders (above the 90th percentile) for anomaly detection and monitoring.
# Interactivity and Insights
# The dashboard allows stakeholders to explore trends, identify top-performing customers, track revenue by product category, and quickly flag potential anomalies, supporting data-driven decision-making.
# Connection Management
# Ensures proper resource cleanup by closing the Snowflake connection after the data is loaded.


import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px

# -----------------------------
# Snowflake Connection
# -----------------------------
conn = snowflake.connector.connect(
    user='XTAINLESSTECH',
    password='jynnaqWowpit2xazqe',
    account='BFZPUDT-WO35181',
    warehouse='MY_WH',
    database='MY_PROJECT',
    schema='ANALYTICS',
    role='ACCOUNTADMIN'
)

# Function to run a query and return a DataFrame
def run_query(query):
    cur = conn.cursor()
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
    cur.close()
    return df

# -----------------------------
# Queries
# -----------------------------
top_customers_query = "SELECT * FROM TOP_CUSTOMERS"
category_sales_query = "SELECT * FROM PRODUCT_CATEGORY_SALES"

# Example: Anomalies (orders above 90th percentile)
anomalies_query = """
SELECT *
FROM FACT_ORDERS_CLEAN
WHERE ORDER_AMOUNT > (
    SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY ORDER_AMOUNT)
    FROM FACT_ORDERS_CLEAN
)
"""

# -----------------------------
# Load Data
# -----------------------------
top_customers = run_query(top_customers_query)
category_sales = run_query(category_sales_query)
anomalies = run_query(anomalies_query)

# -----------------------------
# Data Cleaning / Type Conversion
# -----------------------------
# Convert TOTAL_REVENUE to float (fix for qcut)
top_customers['TOTAL_REVENUE'] = top_customers['TOTAL_REVENUE'].astype(float)

# -----------------------------
# Streamlit Layout
# -----------------------------
st.title("Sales Analytics Dashboard")

# --- Dashboard 1: Sales Overview ---
st.header("Sales Overview")

# Top Customers
fig1 = px.bar(
    top_customers,
    x='CUSTOMER_ID',
    y='TOTAL_REVENUE',
    text='TOTAL_REVENUE',
    title='Top 10 Customers'
)
st.plotly_chart(fig1)

# Product Category Performance
fig2 = px.pie(
    category_sales,
    names='CATEGORY',
    values='CATEGORY_REVENUE',
    title='Revenue by Product Category'
)
st.plotly_chart(fig2)

# --- Dashboard 2: Customer Segmentation ---
st.header("Customer Segmentation")
# Create revenue tiers
top_customers['SEGMENT'] = pd.qcut(
    top_customers['TOTAL_REVENUE'],
    q=3,
    labels=['Low', 'Medium', 'High']
)
fig3 = px.histogram(
    top_customers,
    x='SEGMENT',
    y='TOTAL_REVENUE',
    title='Customer Revenue Segmentation',
    text_auto=True
)
st.plotly_chart(fig3)

# --- Dashboard 3: Anomalies Flagged ---
st.header("Anomalies Flagged")
st.write("Orders above the 90th percentile in value")
st.dataframe(anomalies)

# -----------------------------
# Close Snowflake Connection
# -----------------------------
conn.close()
