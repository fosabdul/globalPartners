import streamlit as st
import pandas as pd

# Use the exact filenames shown in your folder
kpi = pd.read_csv("global_partners_gold.kpi_summary_gold.csv")
cat = pd.read_csv("global_partners_gold.category_revenue_gold.csv")

st.title("Global Partners – Executive Overview")

total_orders = int(kpi["total_orders"].iloc[0])
total_revenue = float(kpi["total_revenue"].iloc[0])
avg_order_value = float(kpi["avg_order_value"].iloc[0])
max_order_value = float(kpi["max_order_value"].iloc[0])

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders", f"{total_orders}")
col2.metric("Total Revenue", f"{total_revenue:,.2f}")
col3.metric("Avg Order Value", f"{avg_order_value:,.2f}")
col4.metric("Max Order Value", f"{max_order_value:,.2f}")

st.markdown("---")

st.subheader("Revenue by Category")
st.dataframe(cat.sort_values("total_revenue", ascending=False))

st.bar_chart(
    cat.sort_values("total_revenue", ascending=False).set_index("category")["total_revenue"]
)
