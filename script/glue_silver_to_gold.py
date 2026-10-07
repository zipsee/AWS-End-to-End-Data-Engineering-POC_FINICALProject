import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ============================================================
# JOB ARGUMENTS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET"]
)

bucket = args["BUCKET"]


# ============================================================
# GLUE / SPARK SETUP
# ============================================================

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ============================================================
# SILVER PATHS
# ============================================================

SILVER_CUSTOMER_PATH = (
    f"s3://{bucket}/silver/customer/"
)

SILVER_TRANSACTION_PATH = (
    f"s3://{bucket}/silver/transaction/"
)


# ============================================================
# GOLD PATHS
# ============================================================

GOLD_CUSTOMER_PATH = (
    f"s3://{bucket}/gold/dim_customer/"
)

GOLD_TRANSACTION_PATH = (
    f"s3://{bucket}/gold/fact_transaction/"
)


# ============================================================
# 1. DIM_CUSTOMER
# ============================================================

print("==============================================")
print("Processing DIM_CUSTOMER")
print("==============================================")

customer_df = spark.read.parquet(
    SILVER_CUSTOMER_PATH
)

dim_customer = (
    customer_df
    .select(
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "city",
        "state",
        "country",
        "created_date",
        "updated_at"
    )
    .withColumn(
        "customer_name",
        F.trim(F.col("customer_name"))
    )
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
)

print("Customer records:", dim_customer.count())

(
    dim_customer
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_CUSTOMER_PATH)
)

print("DIM_CUSTOMER written successfully")


# ============================================================
# 2. FACT_TRANSACTION
# ============================================================

print("==============================================")
print("Processing FACT_TRANSACTION")
print("==============================================")

transaction_df = spark.read.parquet(
    SILVER_TRANSACTION_PATH
)

fact_transaction = (
    transaction_df
    .select(
        "transaction_id",
        "customer_id",
        "account_id",
        "product_id",
        "transaction_date",
        "transaction_type",
        "quantity",
        "price",
        "amount",
        "currency",
        "status",
        "updated_at"
    )
)


# ============================================================
# FACT TABLE CLEANUP
# ============================================================

fact_transaction = (
    fact_transaction

    .withColumn(
        "transaction_id",
        F.trim(F.col("transaction_id"))
    )

    .withColumn(
        "customer_id",
        F.trim(F.col("customer_id"))
    )

    .withColumn(
        "account_id",
        F.trim(F.col("account_id"))
    )

    .withColumn(
        "product_id",
        F.trim(F.col("product_id"))
    )

    .withColumn(
        "transaction_type",
        F.upper(F.trim(F.col("transaction_type")))
    )

    .withColumn(
        "currency",
        F.upper(F.trim(F.col("currency")))
    )

    .withColumn(
        "status",
        F.upper(F.trim(F.col("status")))
    )
)


print(
    "Transaction records:",
    fact_transaction.count()
)


# ============================================================
# WRITE FACT_TRANSACTION
# ============================================================

(
    fact_transaction
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_TRANSACTION_PATH)
)

print("FACT_TRANSACTION written successfully")


# ============================================================
# JOB COMPLETE
# ============================================================

print("==============================================")
print("SILVER TO GOLD COMPLETED SUCCESSFULLY")
print("==============================================")

job.commit()