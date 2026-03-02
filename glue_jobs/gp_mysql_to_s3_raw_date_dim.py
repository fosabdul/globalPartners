import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    explode, sequence, to_date, date_format,
    dayofweek, weekofyear, month, year, lit, when
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
# GENERATE DATE DIMENSION
# ----------------------------
df = spark.sql("""
    SELECT explode(
        sequence(
            to_date('2019-01-01'),
            to_date('2025-12-31'),
            interval 1 day
        )
    ) AS date_key
""")

df = (
    df
    .withColumn("day_of_week",  date_format("date_key", "EEEE"))
    .withColumn("week",         weekofyear("date_key"))
    .withColumn("month_num",    month("date_key"))
    .withColumn("month",        date_format("date_key", "MMMM"))
    .withColumn("year",         year("date_key"))
    .withColumn("is_weekend",   dayofweek("date_key").isin([1, 7]))
    .withColumn("is_holiday",   lit(False))
    .withColumn("holiday_name", lit(None).cast("string"))
)

print("DATE DIM ROW COUNT:", df.count())
df.printSchema()

# ----------------------------
# WRITE TO BRONZE
# ----------------------------
df.write.mode("overwrite").parquet(
    "s3://global-partners-data/bronze/date_dim/"
)

print("✅ Date dim written to bronze.")
job.commit()