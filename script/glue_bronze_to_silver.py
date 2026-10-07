import sys
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ============================================================
# 1. GLUE JOB SETUP
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["BUCKET"]

print("===================================================")
print("FINANCIAL POC - BRONZE TO SILVER")
print("===================================================")
print(f"Bucket: {bucket}")
print(f"Job: {args['JOB_NAME']}")


# ============================================================
# 2. PATHS
# ============================================================

BRONZE_PATH = f"s3://{bucket}/bronze"
SILVER_PATH = f"s3://{bucket}/silver"
QUARANTINE_PATH = f"s3://{bucket}/quarantine"
AUDIT_PATH = f"s3://{bucket}/audit/data_quality"


# ============================================================
# 3. EXPECTED SCHEMAS
# ============================================================

schemas = {

    "customer": StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_date", DateType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),

    "account": StructType([
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("account_status", StringType(), True),
        StructField("opening_date", DateType(), True),
        StructField("balance", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),

    "product": StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("risk_category", StringType(), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_date", DateType(), True)
    ]),

    "transaction": StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),

    "portfolio": StructType([
        StructField("portfolio_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("portfolio_name", StringType(), True),
        StructField("portfolio_type", StringType(), True),
        StructField("total_value", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("valuation_date", DateType(), True),
        StructField("updated_at", TimestampType(), True)
    ])
}


# ============================================================
# 4. PRIMARY KEYS
# ============================================================

primary_keys = {
    "customer": "customer_id",
    "account": "account_id",
    "product": "product_id",
    "transaction": "transaction_id",
    "portfolio": "portfolio_id"
}


# ============================================================
# 5. REQUIRED / NOT NULL COLUMNS
# ============================================================

required_columns = {

    "customer": [
        "customer_id",
        "customer_name",
        "email"
    ],

    "account": [
        "account_id",
        "customer_id",
        "account_type"
    ],

    "product": [
        "product_id",
        "product_name",
        "product_type"
    ],

    "transaction": [
        "transaction_id",
        "customer_id",
        "account_id",
        "product_id",
        "transaction_date",
        "amount"
    ],

    "portfolio": [
        "portfolio_id",
        "customer_id",
        "account_id",
        "valuation_date"
    ]
}


# ============================================================
# 6. HELPER FUNCTION - READ BRONZE
# ============================================================

def read_bronze(entity):

    input_path = f"{BRONZE_PATH}/{entity}/"

    print("---------------------------------------------------")
    print(f"Reading Bronze: {input_path}")
    print("---------------------------------------------------")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(input_path)
    )

    print(f"{entity} Bronze count: {df.count()}")

    return df


# ============================================================
# 7. HELPER FUNCTION - VALIDATE REQUIRED COLUMNS
# ============================================================

def validate_columns(df, entity):

    expected_columns = [
        field.name
        for field in schemas[entity].fields
    ]

    actual_columns = df.columns

    missing_columns = [
        column
        for column in expected_columns
        if column not in actual_columns
    ]

    extra_columns = [
        column
        for column in actual_columns
        if column not in expected_columns
    ]

    if missing_columns:
        raise Exception(
            f"{entity}: Missing columns: {missing_columns}"
        )

    if extra_columns:
        raise Exception(
            f"{entity}: Unexpected columns: {extra_columns}"
        )

    print(f"{entity}: Schema column validation PASSED")


# ============================================================
# 8. HELPER FUNCTION - DATATYPE CONVERSION
# ============================================================

def apply_schema(df, entity):

    schema = schemas[entity]

    for field in schema.fields:

        column_name = field.name

        if isinstance(field.dataType, DateType):

            df = df.withColumn(
                column_name,
                F.to_date(
                    F.col(column_name),
                    "yyyy-MM-dd"
                )
            )

        elif isinstance(field.dataType, TimestampType):

            df = df.withColumn(
                column_name,
                F.to_timestamp(
                    F.col(column_name),
                    "yyyy-MM-dd HH:mm:ss"
                )
            )

        else:

            df = df.withColumn(
                column_name,
                F.col(column_name).cast(field.dataType)
            )

    return df


# ============================================================
# 9. HELPER FUNCTION - NULL VALIDATION
# ============================================================

def find_null_records(df, entity):

    conditions = []

    for column in required_columns[entity]:

        conditions.append(
            F.col(column).isNull() |
            (F.trim(F.col(column).cast("string")) == "")
        )

    condition = conditions[0]

    for current_condition in conditions[1:]:
        condition = condition | current_condition

    bad_df = df.filter(condition)

    good_df = df.filter(~condition)

    return good_df, bad_df


# ============================================================
# 10. HELPER FUNCTION - DUPLICATE VALIDATION
# ============================================================

def find_duplicate_records(df, entity):

    primary_key = primary_keys[entity]

    duplicate_keys = (
        df.groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .select(primary_key)
    )

    duplicate_df = df.join(
        duplicate_keys,
        primary_key,
        "inner"
    )

    good_df = df.join(
        duplicate_keys,
        primary_key,
        "left_anti"
    )

    return good_df, duplicate_df


# ============================================================
# 11. BUSINESS RULE VALIDATION
# ============================================================

def apply_business_rules(df, entity):

    bad_df = None

    if entity == "account":

        bad_df = df.filter(
            (F.col("balance") < 0) |
            (~F.col("account_status").isin(
                "ACTIVE",
                "INACTIVE",
                "CLOSED"
            ))
        )

    elif entity == "product":

        bad_df = df.filter(
            (F.col("interest_rate") < 0) |
            (~F.col("status").isin(
                "ACTIVE",
                "INACTIVE"
            ))
        )

    elif entity == "transaction":

        bad_df = df.filter(
            (F.col("quantity") <= 0) |
            (F.col("price") < 0) |
            (F.col("amount") < 0) |
            (~F.col("status").isin(
                "COMPLETED",
                "PENDING",
                "FAILED"
            ))
        )

    elif entity == "portfolio":

        bad_df = df.filter(
            F.col("total_value") < 0
        )

    else:

        bad_df = df.limit(0)

    good_df = df.subtract(bad_df)

    return good_df, bad_df


# ============================================================
# 12. WRITE QUARANTINE DATA
# ============================================================

def write_quarantine(df, entity, reason):

    if df.limit(1).count() == 0:
        return

    quarantine_df = (
        df
        .withColumn(
            "validation_reason",
            F.lit(reason)
        )
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
    )

    output_path = (
        f"{QUARANTINE_PATH}/{reason}/{entity}/"
    )

    print(
        f"Writing {reason} records for {entity}: "
        f"{output_path}"
    )

    (
        quarantine_df.write
        .mode("append")
        .format("parquet")
        .option("compression", "snappy")
        .save(output_path)
    )


# ============================================================
# 13. PROCESS ENTITY
# ============================================================

def process_entity(entity):

    print("")
    print("===================================================")
    print(f"PROCESSING ENTITY: {entity.upper()}")
    print("===================================================")

    # Read Bronze
    df = read_bronze(entity)

    source_count = df.count()

    print(f"{entity} source records: {source_count}")

    # Schema column validation
    validate_columns(df, entity)

    # Apply explicit schema
    df = apply_schema(df, entity)

    print(f"{entity}: Schema enforcement completed")

    # NULL validation
    good_df, null_df = find_null_records(
        df,
        entity
    )

    null_count = null_df.count()

    print(
        f"{entity}: NULL validation "
        f"failed records = {null_count}"
    )

    write_quarantine(
        null_df,
        entity,
        "null"
    )

    # Duplicate validation
    good_df, duplicate_df = find_duplicate_records(
        good_df,
        entity
    )

    duplicate_count = duplicate_df.count()

    print(
        f"{entity}: Duplicate validation "
        f"failed records = {duplicate_count}"
    )

    write_quarantine(
        duplicate_df,
        entity,
        "duplicate"
    )

    # Business rules
    good_df, business_bad_df = apply_business_rules(
        good_df,
        entity
    )

    business_count = business_bad_df.count()

    print(
        f"{entity}: Business rule "
        f"failed records = {business_count}"
    )

    write_quarantine(
        business_bad_df,
        entity,
        "business_rule"
    )

    # Final Silver count
    silver_count = good_df.count()

    print(
        f"{entity}: Final Silver records = "
        f"{silver_count}"
    )

    # Write Silver
    silver_path = f"{SILVER_PATH}/{entity}/"

    print(
        f"{entity}: Writing Silver -> "
        f"{silver_path}"
    )

    (
        good_df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(silver_path)
    )

    print(
        f"{entity}: Silver write completed"
    )

    # Audit record
    audit_df = spark.createDataFrame(
        [
            (
                entity,
                source_count,
                null_count,
                duplicate_count,
                business_count,
                silver_count,
                datetime.now()
            )
        ],
        [
            "entity",
            "source_count",
            "null_count",
            "duplicate_count",
            "business_rule_count",
            "silver_count",
            "audit_timestamp"
        ]
    )

    (
        audit_df.write
        .mode("append")
        .format("parquet")
        .save(AUDIT_PATH)
    )

    print(
        f"{entity}: Audit information written"
    )


# ============================================================
# 14. MAIN PIPELINE
# ============================================================

entities = [
    "customer",
    "account",
    "product",
    "transaction",
    "portfolio"
]


for entity in entities:

    try:

        process_entity(entity)

    except Exception as error:

        print(
            f"ERROR processing {entity}: "
            f"{str(error)}"
        )

        raise


# ============================================================
# 15. JOB COMPLETION
# ============================================================

print("")
print("===================================================")
print("ALL ENTITIES PROCESSED SUCCESSFULLY")
print("===================================================")

job.commit()

print("Glue job completed successfully.")