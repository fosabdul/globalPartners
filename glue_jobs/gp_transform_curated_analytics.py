import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, sum as _sum, count as _count, countDistinct,
    avg as _avg, max as _max, min as _min,
    datediff, to_date, date_format, round as _round,
    when, lit
)

# ----------------------------
# INIT
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# READ BRONZE DATA
# ----------------------------
bronze_path = "s3://global-partners-data/bronze/order_items/"
df = spark.read.parquet(bronze_path)
print("Bronze Row Count:", df.count())
df.printSchema()

# Parse order date from orderid (which stores the timestamp)
df = df.withColumn(
    "order_date",
    to_date(col("orderid"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

# ----------------------------
# GOLD 1: ORDER METRICS
# (orderid, order_revenue, items_per_order)
# ----------------------------
gold_order_metrics = (
    df.groupBy("orderid")
    .agg(
        _round(_sum(col("itemprice") * col("itemquantity")), 2).alias("order_revenue"),
        _sum(col("itemquantity")).alias("items_per_order")
    )
)
gold_order_metrics.write.mode("overwrite").parquet(
    "s3://global-partners-data/gold/order_metrics/"
)
print("✅ Gold order_metrics written")

# ----------------------------
# GOLD 2: DAILY CLV
# (orderid, order_date, order_revenue)
# ----------------------------
gold_daily_clv = (
    df.groupBy("orderid", "order_date")
    .agg(
        _round(_sum(col("itemprice") * col("itemquantity")), 2).alias("order_revenue")
    )
)
gold_daily_clv.write.mode("overwrite").parquet(
    "s3://global-partners-data/gold/daily_clv/"
)
print("✅ Gold daily_clv written")

# ----------------------------
# GOLD 3: CATEGORY REVENUE
# (itemcategory, total_revenue, total_items)
# ----------------------------
gold_category_revenue = (
    df.groupBy("itemcategory")
    .agg(
        _round(_sum(col("itemprice") * col("itemquantity")), 2).alias("total_revenue"),
        _sum(col("itemquantity")).alias("total_items")
    )
)
gold_category_revenue.write.mode("overwrite").parquet(
    "s3://global-partners-data/gold/category_revenue/"
)
print("✅ Gold category_revenue written")

# ----------------------------
# GOLD 4: RETENTION
# (restaurantid, order_date, orders)
# ----------------------------
gold_retention = (
    df.groupBy("restaurantid", "order_date")
    .agg(
        countDistinct("orderid").alias("orders")
    )
)
gold_retention.write.mode("overwrite").parquet(
    "s3://global-partners-data/gold/retention/"
)
print("✅ Gold retention written")

# ----------------------------
# GOLD 5: COHORT
# (restaurantid, cohort_month)
# ----------------------------
gold_cohort = (
    df.groupBy("restaurantid")
    .agg(
        date_format(_min("order_date"), "yyyy-MM").alias("cohort_month")
    )
)
gold_cohort.write.mode("overwrite").parquet(
    "s3://global-partners-data/gold/cohort/"
)
print("✅ Gold cohort written")

job.commit()
print("🎉 All gold tables written successfully.")