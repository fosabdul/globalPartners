import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

raw_df = (
    spark.read.format("jdbc")
        .option("url", "jdbc:sqlserver://database-globalpartners.cel8iuwikcnt.us-east-1.rds.amazonaws.com:1433;databaseName=global_partners")
        .option("dbtable", "dbo.order_item_options")
        .option("user", "admin")
        .option("password", "foska2026")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
)

print("ROW COUNT:", raw_df.count())
print("COLUMNS:", raw_df.columns)

# Rename columns to lowercase standard
df = raw_df
rename_map = {
    "ORDER_ID":          "orderid",
    "LINEITEM_ID":       "lineitemid",
    "OPTION_GROUP_NAME": "optiongroupname",
    "OPTION_NAME":       "optionname",
    "OPTION_PRICE":      "optionprice",
    "OPTION_QUANTITY":   "optionquantity",
}
for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# Cast to correct types
df = (
    df
    .withColumn("orderid",        col("orderid").cast("string"))
    .withColumn("lineitemid",     col("lineitemid").cast("string"))
    .withColumn("optionprice",    col("optionprice").cast("decimal(10,2)"))
    .withColumn("optionquantity", col("optionquantity").cast("integer"))
)

print("FINAL SCHEMA:")
df.printSchema()

# Write to Bronze S3
df.write.mode("overwrite").parquet(
    "s3://global-partners-data/bronze/order_item_options/"
)

print("✅ order_item_options written to bronze successfully.")
job.commit()