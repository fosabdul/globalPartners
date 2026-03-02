import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, date_format

# ------------------------------------------------
# Init
# ------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------
# JDBC connection
# ------------------------------------------------
jdbc_url = (
    "jdbc:sqlserver://database-globalpartners.cel8iuwikcnt.us-east-1.rds.amazonaws.com:1433;"
    "databaseName=global_partners"
)
connection_props = {
    "user": "admin",
    "password": "foska2026",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# ------------------------------------------------
# READ SQL TABLE
# ------------------------------------------------
raw_df = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dbo.order_items")
        .options(**connection_props)
        .load()
)

if raw_df is None or len(raw_df.columns) == 0:
    raise Exception("SQL table did not load. Check table name or connection.")

print("ROW COUNT:", raw_df.count())
print("COLUMNS:", raw_df.columns)

# ------------------------------------------------
# Rename columns to standardized names
# ------------------------------------------------
df = raw_df

rename_map = {
    "app_name":             "appname",
    "restaurant_id":        "restaurantid",
    "creation_time_utc":    "creationtimeutc",
    "order_id":             "orderid",
    "user_id":              "userid",
    "printed_card_number":  "printedcardnumber",
    "is_loyalty":           "isloyalty",
    "lineitem_id":          "lineitemid",
    "item_category":        "itemcategory",
    "item_name":            "itemname",
    "item_price":           "itemprice",
    "item_quantity":        "itemquantity",
}
for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# ------------------------------------------------
# Format timestamp
# ------------------------------------------------
if "creationtimeutc" in df.columns:
    df = df.withColumn(
        "creationtimeutc",
        date_format(col("creationtimeutc"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )

# ------------------------------------------------
# Select ALL required columns
# ------------------------------------------------
final_cols = [
    "appname",
    "restaurantid",
    "creationtimeutc",
    "orderid",
    "userid",
    "printedcardnumber",
    "isloyalty",
    "currency",
    "lineitemid",
    "itemcategory",
    "itemname",
    "itemprice",
    "itemquantity",
]
existing_cols = [c for c in final_cols if c in df.columns]
df = df.select(*existing_cols)

# ------------------------------------------------
# Cast to correct types
# ------------------------------------------------
df = (
    df
    .withColumn("restaurantid",     col("restaurantid").cast("string"))
    .withColumn("orderid",          col("orderid").cast("string"))
    .withColumn("userid",           col("userid").cast("string"))
    .withColumn("lineitemid",       col("lineitemid").cast("integer"))
    .withColumn("itemprice",        col("itemprice").cast("decimal(10,2)"))
    .withColumn("itemquantity",     col("itemquantity").cast("integer"))
    .withColumn("isloyalty",        col("isloyalty").cast("boolean"))
)

# ------------------------------------------------
# Write Bronze parquet (raw — all columns preserved)
# ------------------------------------------------
df.write.mode("overwrite").parquet(
    "s3://global-partners-data/bronze/order_items/"
)

print("Bronze write complete.")
job.commit()