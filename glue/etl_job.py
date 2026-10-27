"""AWS Glue Spark ETL job: CSV in S3 -> cleaned, partitioned Parquet in S3."""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "INPUT_BUCKET", "INPUT_KEY", "RAW_BUCKET",
     "CURATED_BUCKET", "CURATED_PREFIX"],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

input_path = f"s3://{args['INPUT_BUCKET']}/{args['INPUT_KEY']}"
output_path = f"s3://{args['CURATED_BUCKET']}/{args['CURATED_PREFIX'].strip('/')}"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(input_path)
)

# Normalize the expected columns before applying transformations.
expected = ["order_id", "customer_id", "order_date", "amount", "status"]
missing = [column for column in expected if column not in df.columns]
if missing:
    raise ValueError(f"Input CSV is missing required columns: {missing}")

cleaned = (
    df.select(*expected)
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("order_date", F.to_date(F.trim(F.col("order_date")), "yyyy-MM-dd"))
    .withColumn(
        "amount",
        F.regexp_replace(F.trim(F.col("amount")), "[$,]", "").cast(DecimalType(12, 2)),
    )
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
    .filter(F.col("order_date").isNotNull())
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
)

(
    cleaned.write
    .mode("append")
    .format("parquet")
    .partitionBy("order_year", "order_month")
    .save(output_path)
)

job.commit()
