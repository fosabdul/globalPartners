# PURPOSE: Streamlit dashboard visualizing GlobalPartners Gold analytics datasets via Athena

import streamlit as st
import boto3
import pandas as pd
import time

ATHENA_DB = "default"
ATHENA_OUTPUT = "s3://global-partners-data/athena-results/"

session = boto3.Session(region_name="us-east-1")
athena = session.client("athena")


# PURPOSE: Helper function to run Athena query and return dataframe
def run_athena_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    qid = response["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        st.error(f"Athena query failed: {state}")
        return pd.DataFrame()

    results = athena.get_query_results(QueryExecutionId=qid)

    cols = [c["Label"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in results["ResultSet"]["Rows"][1:]:
        rows.append([f.get("VarCharValue") for f in row["Data"]])

    return pd.DataFrame(rows, columns=cols)


st.title("GlobalPartners Analytics Dashboard")

# =========================
# PURPOSE: Category Revenue KPIs
# =========================
category_df = run_athena_query("""
SELECT itemcategory,
       CAST(total_revenue AS DOUBLE) AS total_revenue,
       CAST(total_items AS INT) AS total_items
FROM globalpartners_gold_category_revenue
ORDER BY total_revenue DESC
""")

category_df["total_revenue"] = pd.to_numeric(category_df["total_revenue"], errors="coerce")
category_df["total_items"] = pd.to_numeric(category_df["total_items"], errors="coerce")

# PURPOSE: Styled category revenue table with colors

st.subheader("Revenue by Category")

styled_category = category_df.style \
    .background_gradient(subset=["total_revenue"], cmap="Greens") \
    .background_gradient(subset=["total_items"], cmap="Blues") \
    .format({"total_revenue": "${:,.0f}", "total_items": "{:,}"})

st.dataframe(styled_category, use_container_width=True)

st.bar_chart(category_df.set_index("itemcategory")["total_revenue"])


# =========================
# PURPOSE: Daily CLV Trend
# =========================
clv_df = run_athena_query("""
SELECT order_date,
       CAST(order_revenue AS DOUBLE) AS order_revenue
FROM globalpartners_gold_daily_clv
ORDER BY order_date
""")

clv_df["order_revenue"] = pd.to_numeric(clv_df["order_revenue"], errors="coerce")

st.subheader("Daily Revenue Trend (CLV Proxy)")
# st.line_chart(clv_df.set_index("order_date")["order_revenue"])

st.area_chart(clv_df.set_index("order_date")["order_revenue"])

# =========================
# PURPOSE: Retention dataset
# =========================
retention_df = run_athena_query("""
SELECT order_date,
       CAST(orders AS INT) AS orders
FROM globalpartners_gold_retention
ORDER BY order_date
""")

retention_df["orders"] = pd.to_numeric(retention_df["orders"], errors="coerce")

st.subheader("Order Activity (Retention Proxy)")
st.line_chart(retention_df.set_index("order_date")["orders"])

# =========================
# PURPOSE: Cohort dataset
# =========================
cohort_df = run_athena_query("""
SELECT cohort_month,
       COUNT(*) AS restaurants
FROM globalpartners_gold_cohort
GROUP BY cohort_month
ORDER BY cohort_month
""")

st.subheader("Cohort Growth")
# st.bar_chart(cohort_df.set_index("cohort_month")["restaurants"])

st.bar_chart(cohort_df.set_index("cohort_month")["restaurants"], color="#FF7F50")
st.dataframe(cohort_df)