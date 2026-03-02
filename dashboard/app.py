# PURPOSE: GlobalPartners Single Page Executive Dashboard

import streamlit as st
import boto3
import pandas as pd
import time

st.set_page_config(layout="wide")

ATHENA_DB = "default"
ATHENA_OUTPUT = "s3://global-partners-data/athena-results/"

session = boto3.Session(region_name="us-east-1")
athena = session.client("athena")


# =========================
# Athena helper
# =========================
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
        st.warning("Athena query failed")
        return pd.DataFrame()

    results = athena.get_query_results(QueryExecutionId=qid)
    cols = [c["Label"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in results["ResultSet"]["Rows"][1:]:
        rows.append([f.get("VarCharValue") for f in row["Data"]])

    return pd.DataFrame(rows, columns=cols)


st.title("GlobalPartners Executive Analytics Dashboard")

# =====================================================
# 0️⃣ CLV — PRIMARY METRIC
# =====================================================
st.header("Customer Lifetime Value (CLV)")
st.caption("ℹ️ userid is not populated in source data. CLV is calculated at order level combining item revenue and option add-ons/discounts.")

clv = run_athena_query("""
WITH order_items_revenue AS (
    SELECT
        orderid,
        SUM(CAST(itemprice AS DOUBLE) * itemquantity) AS items_revenue
    FROM globalpartners_bronze_order_items
    WHERE orderid IS NOT NULL
    GROUP BY orderid
),
order_options_revenue AS (
    SELECT
        orderid,
        SUM(CAST(optionprice AS DOUBLE) * optionquantity) AS options_revenue
    FROM globalpartners_bronze_order_item_options
    WHERE orderid IS NOT NULL
    GROUP BY orderid
),
order_clv AS (
    SELECT
        i.orderid,
        CAST(
            CAST(regexp_replace(regexp_replace(i.orderid, 'T', ' '), 'Z$', '') AS TIMESTAMP)
        AS DATE) AS order_date,
        ROUND(i.items_revenue + COALESCE(o.options_revenue, 0), 2) AS total_clv
    FROM order_items_revenue i
    LEFT JOIN order_options_revenue o ON i.orderid = o.orderid
),
percentiles AS (
    SELECT
        APPROX_PERCENTILE(total_clv, 0.20) AS p20,
        APPROX_PERCENTILE(total_clv, 0.80) AS p80
    FROM order_clv
)
SELECT
    o.orderid,
    CAST(o.order_date AS VARCHAR) AS order_date,
    o.total_clv,
    CASE
        WHEN o.total_clv >= p.p80 THEN 'High'
        WHEN o.total_clv <= p.p20 THEN 'Low'
        ELSE 'Medium'
    END AS clv_segment
FROM order_clv o
CROSS JOIN percentiles p
ORDER BY o.total_clv DESC
""")

if not clv.empty:
    clv["total_clv"] = pd.to_numeric(clv["total_clv"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Orders",      f"{len(clv):,}")
    c2.metric("Avg Order CLV",     f"${clv['total_clv'].mean():,.2f}")
    c3.metric("Top Order CLV",     f"${clv['total_clv'].max():,.2f}")
    c4.metric("High Value Orders", f"{(clv['clv_segment']=='High').sum():,}")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("CLV Segment Distribution")
        st.bar_chart(clv["clv_segment"].value_counts().reindex(["High", "Medium", "Low"]), color="#4CB5AE")
    with col2:
        st.subheader("Top 20 Orders by CLV")
        st.dataframe(
            clv.head(20)[["orderid", "order_date", "total_clv", "clv_segment"]].reset_index(drop=True),
            use_container_width=True
        )

st.divider()

# =====================================================
# 1️⃣ CUSTOMER SEGMENTATION (RFM)
# =====================================================
st.header("Customer Segmentation (RFM)")
st.caption("ℹ️ Restaurants are used as the customer entity. userid is null in source data.")

rfm = run_athena_query("""
WITH reference_date AS (
    SELECT MAX(
        CAST(regexp_replace(regexp_replace(orderid, 'T', ' '), 'Z$', '') AS TIMESTAMP)
    ) AS max_date
    FROM globalpartners_bronze_order_items
    WHERE orderid IS NOT NULL
),
rfm_base AS (
    SELECT
        restaurantid,
        DATE_DIFF('day',
            MAX(CAST(regexp_replace(regexp_replace(orderid, 'T', ' '), 'Z$', '') AS TIMESTAMP)),
            (SELECT max_date FROM reference_date)
        ) AS recency_days,
        COUNT(DISTINCT orderid) AS frequency,
        ROUND(SUM(CAST(itemprice AS DOUBLE) * itemquantity), 2) AS monetary
    FROM globalpartners_bronze_order_items
    WHERE restaurantid IS NOT NULL AND orderid IS NOT NULL
    GROUP BY restaurantid
),
percentiles AS (
    SELECT
        APPROX_PERCENTILE(recency_days, 0.33) AS r_p33,
        APPROX_PERCENTILE(recency_days, 0.66) AS r_p66,
        APPROX_PERCENTILE(frequency,    0.33) AS f_p33,
        APPROX_PERCENTILE(frequency,    0.66) AS f_p66,
        APPROX_PERCENTILE(monetary,     0.33) AS m_p33,
        APPROX_PERCENTILE(monetary,     0.66) AS m_p66
    FROM rfm_base
)
SELECT
    r.restaurantid,
    r.recency_days,
    r.frequency,
    r.monetary,
    CASE
        WHEN r.recency_days <= p.r_p33 AND r.frequency >= p.f_p66 AND r.monetary >= p.m_p66 THEN 'VIP'
        WHEN r.recency_days <= p.r_p33 AND r.frequency <= p.f_p33 THEN 'New Customer'
        WHEN r.recency_days >= p.r_p66 AND r.frequency <= p.f_p33 THEN 'Churn Risk'
        ELSE 'Active'
    END AS rfm_segment
FROM rfm_base r
CROSS JOIN percentiles p
ORDER BY r.monetary DESC
""")

if not rfm.empty:
    rfm["recency_days"] = pd.to_numeric(rfm["recency_days"])
    rfm["frequency"]    = pd.to_numeric(rfm["frequency"])
    rfm["monetary"]     = pd.to_numeric(rfm["monetary"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Restaurants", f"{len(rfm):,}")
    c2.metric("VIP Restaurants",   f"{(rfm['rfm_segment']=='VIP').sum():,}")
    c3.metric("Churn Risk",        f"{(rfm['rfm_segment']=='Churn Risk').sum():,}")
    c4.metric("Avg Monetary",      f"${rfm['monetary'].mean():,.2f}")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("RFM Segment Distribution")
        st.bar_chart(rfm["rfm_segment"].value_counts(), color="#4CB5AE")
    with col2:
        st.subheader("Top 20 Restaurants by Spend")
        st.dataframe(
            rfm.head(20)[["restaurantid", "recency_days", "frequency", "monetary", "rfm_segment"]].reset_index(drop=True),
            use_container_width=True
        )

    st.subheader("Avg Monetary Value by Segment")
    st.bar_chart(rfm.groupby("rfm_segment")["monetary"].mean().round(2), color="#F28E2B")

st.divider()

# =====================================================
# 2️⃣ CHURN RISK
# =====================================================
st.header("Churn Risk Indicators")

churn = run_athena_query("""
WITH restaurant_activity AS (
    SELECT
        restaurantid,
        MAX(order_date)                                    AS last_order_date,
        COUNT(DISTINCT order_date)                         AS total_active_days,
        SUM(orders)                                        AS total_orders,
        DATE_DIFF('day', MIN(order_date), MAX(order_date)) AS days_span
    FROM globalpartners_gold_retention
    GROUP BY restaurantid
),
reference AS (
    SELECT MAX(order_date) AS max_date FROM globalpartners_gold_retention
),
churn_metrics AS (
    SELECT
        a.restaurantid,
        a.last_order_date,
        DATE_DIFF('day', a.last_order_date, r.max_date) AS days_since_last_order,
        a.total_orders,
        CASE
            WHEN a.total_active_days > 1
            THEN ROUND(CAST(a.days_span AS DOUBLE) / (a.total_active_days - 1), 1)
            ELSE NULL
        END AS avg_order_gap_days
    FROM restaurant_activity a
    CROSS JOIN reference r
)
SELECT
    restaurantid,
    CAST(last_order_date AS VARCHAR) AS last_order_date,
    days_since_last_order,
    total_orders,
    avg_order_gap_days,
    CASE
        WHEN days_since_last_order > 45 THEN 'At Risk'
        WHEN days_since_last_order > 20 THEN 'Watch'
        ELSE 'Active'
    END AS churn_status
FROM churn_metrics
ORDER BY days_since_last_order DESC
""")

if not churn.empty:
    churn["days_since_last_order"] = pd.to_numeric(churn["days_since_last_order"])
    churn["total_orders"]          = pd.to_numeric(churn["total_orders"])
    churn["avg_order_gap_days"]    = pd.to_numeric(churn["avg_order_gap_days"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Restaurants",    f"{len(churn):,}")
    c2.metric("At Risk (>45 days)",   f"{(churn['churn_status']=='At Risk').sum():,}")
    c3.metric("Watch (>20 days)",     f"{(churn['churn_status']=='Watch').sum():,}")
    c4.metric("Avg Days Since Order", f"{churn['days_since_last_order'].mean():.0f}")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Churn Status Distribution")
        st.bar_chart(
            churn["churn_status"].value_counts().reindex(["At Risk", "Watch", "Active"]),
            color="#E15759"
        )
    with col2:
        st.subheader("At Risk Restaurants")
        st.dataframe(
            churn[churn["churn_status"] == "At Risk"][
                ["restaurantid", "last_order_date", "days_since_last_order", "avg_order_gap_days"]
            ].reset_index(drop=True),
            use_container_width=True
        )

    st.subheader("Days Since Last Order Distribution")
    st.bar_chart(churn["days_since_last_order"].value_counts().sort_index(), color="#F28E2B")

st.divider()

# =====================================================
# 3️⃣ SALES TRENDS & SEASONALITY
# =====================================================
st.header("Sales Trends & Seasonality")

daily_sales = run_athena_query("""
SELECT d.order_date, d.order_revenue, dd.day_of_week, dd.month, dd.year, dd.is_weekend
FROM globalpartners_gold_daily_clv d
LEFT JOIN globalpartners_bronze_date_dim dd ON d.order_date = dd.date_key
ORDER BY d.order_date
""")

monthly_sales = run_athena_query("""
SELECT
    dd.month, dd.month_num, dd.year,
    ROUND(SUM(d.order_revenue), 2) AS monthly_revenue
FROM globalpartners_gold_daily_clv d
LEFT JOIN globalpartners_bronze_date_dim dd ON d.order_date = dd.date_key
WHERE dd.month IS NOT NULL
GROUP BY dd.month, dd.month_num, dd.year
ORDER BY dd.year, dd.month_num
""")

category_sales = run_athena_query("""
SELECT itemcategory, CAST(total_revenue AS DOUBLE) AS total_revenue
FROM globalpartners_gold_category_revenue
WHERE itemcategory IS NOT NULL AND itemcategory != ''
ORDER BY total_revenue DESC
""")

weekend_sales = run_athena_query("""
SELECT
    CASE WHEN dd.is_weekend = true THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    ROUND(SUM(d.order_revenue), 2) AS total_revenue
FROM globalpartners_gold_daily_clv d
LEFT JOIN globalpartners_bronze_date_dim dd ON d.order_date = dd.date_key
WHERE dd.is_weekend IS NOT NULL
GROUP BY dd.is_weekend
""")

if not daily_sales.empty:
    daily_sales["order_revenue"] = pd.to_numeric(daily_sales["order_revenue"])
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Revenue",      f"${daily_sales['order_revenue'].sum():,.0f}")
    c2.metric("Avg Daily Revenue",  f"${daily_sales['order_revenue'].mean():,.2f}")
    c3.metric("Peak Daily Revenue", f"${daily_sales['order_revenue'].max():,.2f}")
    st.subheader("Daily Revenue Trend")
    st.area_chart(daily_sales.set_index("order_date")["order_revenue"], color="#4CB5AE")

if not monthly_sales.empty:
    monthly_sales["monthly_revenue"] = pd.to_numeric(monthly_sales["monthly_revenue"])
    monthly_sales["month_label"] = monthly_sales["month"] + " " + monthly_sales["year"]
    st.subheader("Monthly Revenue Trend")
    st.bar_chart(monthly_sales.set_index("month_label")["monthly_revenue"], color="#F28E2B")

col1, col2 = st.columns(2)
if not category_sales.empty:
    category_sales["total_revenue"] = pd.to_numeric(category_sales["total_revenue"])
    with col1:
        st.subheader("Revenue by Menu Category")
        st.bar_chart(category_sales.set_index("itemcategory")["total_revenue"], color="#59A14F")

if not weekend_sales.empty:
    weekend_sales["total_revenue"] = pd.to_numeric(weekend_sales["total_revenue"])
    with col2:
        st.subheader("Weekday vs Weekend Revenue")
        st.bar_chart(weekend_sales.set_index("day_type")["total_revenue"], color="#EDC948")

st.divider()

# =====================================================
# 4️⃣ LOYALTY PROGRAM IMPACT
# =====================================================
st.header("Loyalty Program Impact")
st.caption("ℹ️ isloyalty field is null in source data. Analysis is based on order frequency and spend patterns per restaurant.")

loyalty_proxy = run_athena_query("""
WITH restaurant_metrics AS (
    SELECT
        restaurantid,
        COUNT(DISTINCT orderid) AS total_orders,
        ROUND(SUM(CAST(itemprice AS DOUBLE) * itemquantity), 2) AS total_spend,
        ROUND(AVG(CAST(itemprice AS DOUBLE) * itemquantity), 2) AS avg_item_spend,
        COUNT(DISTINCT orderid) /
            NULLIF(DATE_DIFF('day',
                MIN(CAST(regexp_replace(regexp_replace(orderid,'T',' '),'Z$','') AS TIMESTAMP)),
                MAX(CAST(regexp_replace(regexp_replace(orderid,'T',' '),'Z$','') AS TIMESTAMP))
            ), 0) * 30.0 AS orders_per_month
    FROM globalpartners_bronze_order_items
    WHERE restaurantid IS NOT NULL AND orderid IS NOT NULL
    GROUP BY restaurantid
),
percentiles AS (
    SELECT APPROX_PERCENTILE(total_orders, 0.75) AS p75
    FROM restaurant_metrics
)
SELECT
    r.restaurantid,
    r.total_orders,
    r.total_spend,
    r.avg_item_spend,
    ROUND(r.orders_per_month, 2) AS orders_per_month,
    CASE
        WHEN r.total_orders >= p.p75 THEN 'High Frequency'
        ELSE 'Low Frequency'
    END AS frequency_segment
FROM restaurant_metrics r
CROSS JOIN percentiles p
ORDER BY r.total_orders DESC
""")

if not loyalty_proxy.empty:
    loyalty_proxy["total_orders"]     = pd.to_numeric(loyalty_proxy["total_orders"])
    loyalty_proxy["total_spend"]      = pd.to_numeric(loyalty_proxy["total_spend"])
    loyalty_proxy["avg_item_spend"]   = pd.to_numeric(loyalty_proxy["avg_item_spend"])
    loyalty_proxy["orders_per_month"] = pd.to_numeric(loyalty_proxy["orders_per_month"])

    high = loyalty_proxy[loyalty_proxy["frequency_segment"] == "High Frequency"]
    low  = loyalty_proxy[loyalty_proxy["frequency_segment"] == "Low Frequency"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("High Frequency Restaurants", f"{len(high):,}")
    c2.metric("Avg Spend (High Freq)",      f"${high['total_spend'].mean():,.2f}")
    c3.metric("Low Frequency Restaurants",  f"{len(low):,}")
    c4.metric("Avg Spend (Low Freq)",       f"${low['total_spend'].mean():,.2f}")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Order Frequency Segment Distribution")
        st.bar_chart(loyalty_proxy["frequency_segment"].value_counts(), color="#59A14F")
    with col2:
        st.subheader("Top 20 Restaurants by Order Frequency")
        st.dataframe(
            loyalty_proxy.head(20)[["restaurantid", "total_orders", "total_spend", "orders_per_month", "frequency_segment"]].reset_index(drop=True),
            use_container_width=True
        )

st.divider()

# =====================================================
# 5️⃣ LOCATION PERFORMANCE
# =====================================================
st.header("Location Performance")

location = run_athena_query("""
WITH restaurant_revenue AS (
    SELECT
        restaurantid,
        ROUND(SUM(CAST(itemprice AS DOUBLE) * itemquantity), 2) AS total_revenue,
        COUNT(DISTINCT orderid)                                  AS total_orders,
        ROUND(AVG(CAST(itemprice AS DOUBLE) * itemquantity), 2) AS avg_order_value,
        COUNT(DISTINCT CAST(
            CAST(regexp_replace(regexp_replace(orderid,'T',' '),'Z$','') AS TIMESTAMP)
        AS DATE)) AS active_days
    FROM globalpartners_bronze_order_items
    WHERE restaurantid IS NOT NULL AND orderid IS NOT NULL
    GROUP BY restaurantid
),
cohort AS (
    SELECT restaurantid, cohort_month
    FROM globalpartners_gold_cohort
)
SELECT
    r.restaurantid,
    c.cohort_month,
    r.total_revenue,
    r.total_orders,
    r.avg_order_value,
    r.active_days,
    RANK() OVER (ORDER BY r.total_revenue DESC) AS revenue_rank
FROM restaurant_revenue r
LEFT JOIN cohort c ON r.restaurantid = c.restaurantid
ORDER BY r.total_revenue DESC
""")

if not location.empty:
    location["total_revenue"]   = pd.to_numeric(location["total_revenue"])
    location["total_orders"]    = pd.to_numeric(location["total_orders"])
    location["avg_order_value"] = pd.to_numeric(location["avg_order_value"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Locations",      f"{len(location):,}")
    c2.metric("Top Location Revenue", f"${location['total_revenue'].max():,.2f}")
    c3.metric("Avg Location Revenue", f"${location['total_revenue'].mean():,.2f}")
    c4.metric("Avg Order Value",      f"${location['avg_order_value'].mean():,.2f}")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 20 Locations by Revenue")
        st.bar_chart(location.head(20).set_index("restaurantid")["total_revenue"], color="#EDC948")
    with col2:
        st.subheader("Location Rankings Table")
        st.dataframe(
            location.head(20)[["revenue_rank", "restaurantid", "cohort_month", "total_revenue", "total_orders", "avg_order_value"]].reset_index(drop=True),
            use_container_width=True
        )

st.divider()

# =====================================================
# 6️⃣ PRICING & DISCOUNT EFFECTIVENESS
# =====================================================
st.header("Pricing & Discount Effectiveness")

pricing = run_athena_query("""
WITH order_totals AS (
    SELECT
        i.orderid,
        SUM(CAST(i.itemprice AS DOUBLE) * i.itemquantity) AS gross_revenue,
        COALESCE(SUM(CASE WHEN o.optionprice < 0 THEN CAST(o.optionprice AS DOUBLE) * o.optionquantity ELSE 0 END), 0) AS discount_amount,
        COALESCE(SUM(CASE WHEN o.optionprice > 0 THEN CAST(o.optionprice AS DOUBLE) * o.optionquantity ELSE 0 END), 0) AS addon_revenue,
        MAX(CASE WHEN o.optionprice < 0 THEN 1 ELSE 0 END) AS is_discounted
    FROM globalpartners_bronze_order_items i
    LEFT JOIN globalpartners_bronze_order_item_options o ON i.orderid = o.orderid
    WHERE i.orderid IS NOT NULL
    GROUP BY i.orderid
)
SELECT
    CASE WHEN is_discounted = 1 THEN 'Discounted' ELSE 'Full Price' END AS order_type,
    COUNT(*)                                                              AS total_orders,
    ROUND(SUM(gross_revenue), 2)                                         AS gross_revenue,
    ROUND(SUM(discount_amount), 2)                                       AS total_discounts,
    ROUND(SUM(gross_revenue + discount_amount + addon_revenue), 2)       AS net_revenue,
    ROUND(AVG(gross_revenue), 2)                                         AS avg_order_value
FROM order_totals
GROUP BY is_discounted
""")

category_pricing = run_athena_query("""
SELECT
    itemcategory,
    ROUND(SUM(CAST(itemprice AS DOUBLE) * itemquantity), 2) AS total_revenue,
    SUM(itemquantity)                                        AS total_items,
    ROUND(AVG(CAST(itemprice AS DOUBLE)), 2)                AS avg_price,
    COUNT(DISTINCT orderid)                                  AS total_orders
FROM globalpartners_bronze_order_items
WHERE itemcategory IS NOT NULL AND itemcategory != ''
GROUP BY itemcategory
ORDER BY total_revenue DESC
""")

if not pricing.empty:
    pricing["total_orders"]   = pd.to_numeric(pricing["total_orders"])
    pricing["gross_revenue"]  = pd.to_numeric(pricing["gross_revenue"])
    pricing["net_revenue"]    = pd.to_numeric(pricing["net_revenue"])
    pricing["avg_order_value"] = pd.to_numeric(pricing["avg_order_value"])

    discounted = pricing[pricing["order_type"] == "Discounted"]
    full_price = pricing[pricing["order_type"] == "Full Price"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Discounted Orders",    f"{discounted['total_orders'].sum():,.0f}" if not discounted.empty else "0")
    c2.metric("Full Price Orders",    f"{full_price['total_orders'].sum():,.0f}" if not full_price.empty else "0")
    c3.metric("Total Discount Given", f"${abs(pd.to_numeric(pricing['total_discounts']).sum()):,.2f}")
    c4.metric("Net Revenue",          f"${pricing['net_revenue'].sum():,.2f}")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Discounted vs Full Price Orders")
        st.bar_chart(pricing.set_index("order_type")["total_orders"], color="#E15759")
    with col2:
        st.subheader("Gross vs Net Revenue by Order Type")
        rev_comparison = pricing[["order_type", "gross_revenue", "net_revenue"]].set_index("order_type")
        st.bar_chart(rev_comparison, color=["#4CB5AE", "#F28E2B"])

if not category_pricing.empty:
    category_pricing["total_revenue"] = pd.to_numeric(category_pricing["total_revenue"])
    category_pricing["avg_price"]     = pd.to_numeric(category_pricing["avg_price"])

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Revenue by Category")
        st.bar_chart(category_pricing.set_index("itemcategory")["total_revenue"], color="#59A14F")
    with col2:
        st.subheader("Avg Price by Category")
        st.bar_chart(category_pricing.set_index("itemcategory")["avg_price"], color="#EDC948")

    st.subheader("Category Performance Summary")
    st.dataframe(
        category_pricing[["itemcategory", "total_revenue", "total_items", "avg_price", "total_orders"]].reset_index(drop=True),
        use_container_width=True
    )