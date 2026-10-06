# AWS End-to-End Data Engineering POC

## 📌 Project Overview

This project is an **end-to-end AWS Data Engineering POC** designed to simulate a real-world financial data platform.

The objective is to build a production-style data pipeline that handles:

- Multiple data sources
- File ingestion
- Data validation
- Data quality checks
- ETL processing
- Incremental processing
- CDC
- SCD Type 2
- Data transformation
- Data lake architecture
- Star schema
- Data partitioning
- Error handling
- Monitoring
- Audit logging
- Data reconciliation
- Failure recovery
- Cost and performance optimization
- Infrastructure as Code

The POC intentionally includes **production failure scenarios** so that each issue can be reproduced, investigated, fixed, and documented.

---

# 🏗️ Architecture

```text
                         ┌──────────────────────┐
                         │      DATA SOURCES    │
                         ├──────────────────────┤
                         │ PostgreSQL / MySQL   │
                         │ CSV / JSON Files     │
                         │ REST API             │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │     Amazon S3        │
                         │      LANDING         │
                         └──────────┬───────────┘
                                    │
                              S3 ObjectCreated
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   AWS Lambda         │
                         │ Pre-Validation       │
                         └──────────┬───────────┘
                                    │
                     ┌──────────────┴──────────────┐
                     │                             │
                  PASS                           FAIL
                     │                             │
                     ▼                             ▼
             ┌──────────────┐             ┌────────────────┐
             │    BRONZE    │             │    REJECTED    │
             │    LAYER     │             │     FILES      │
             └──────┬───────┘             └────────────────┘
                    │
                    ▼
          ┌──────────────────────┐
          │   AWS Glue / EMR     │
          │      PySpark         │
          └──────────┬───────────┘
                     │
                     ▼
             DATA VALIDATION
                     │
              ┌──────┴──────┐
              │             │
             PASS          FAIL
              │             │
              ▼             ▼
       TRANSFORMATION    QUARANTINE
              │
              ▼
       POST TRANSFORMATION
          VALIDATION
              │
              ▼
        ┌──────────────┐
        │    SILVER    │
        │    LAYER     │
        └──────┬───────┘
               │
               ▼
        BUSINESS LOGIC
        / SCD2 / CDC
               │
               ▼
          ┌──────────┐
          │   GOLD   │
          │   LAYER  │
          └────┬─────┘
               │
               ▼
      POST-LOAD VALIDATION
               │
               ▼
          ┌──────────┐
          │ Athena   │
          └────┬─────┘
               │
               ▼
        ┌────────────┐
        │ QuickSight │
        └────────────┘
```

---

# ☁️ AWS Services

| Service | Purpose |
|---|---|
| Amazon S3 | Data Lake Storage |
| AWS Lambda | S3 event trigger and pre-validation |
| AWS Glue | ETL, Data Quality and Catalog |
| Amazon EMR | Large-scale Spark processing |
| AWS Step Functions | Pipeline orchestration |
| Amazon EventBridge | Scheduling/event routing when required |
| Amazon Athena | Serverless SQL analytics |
| Amazon QuickSight | Dashboard and visualization |
| Amazon CloudWatch | Logs and monitoring |
| Amazon SNS | Notifications |
| Amazon SQS | Error handling / DLQ |
| Amazon DynamoDB | Pipeline audit metadata |
| AWS IAM | Access control |
| AWS Glue Data Catalog | Metadata/catalog |
| AWS Lake Formation | Data governance |
| Terraform | Infrastructure as Code |
| Jenkins / GitHub Actions | CI/CD |

---

# 🗂️ S3 Data Lake Structure

Bucket:

```text
s3://financial-data-engineering-poc-<unique-name>/
```

Structure:

```text
financial-data-engineering-poc/
│
├── landing/
│   ├── customer/
│   ├── account/
│   ├── product/
│   ├── transaction/
│   └── portfolio/
│
├── bronze/
│   ├── customer/
│   ├── account/
│   ├── product/
│   ├── transaction/
│   └── portfolio/
│
├── silver/
│   ├── customer/
│   ├── account/
│   ├── product/
│   ├── transaction/
│   └── portfolio/
│
├── gold/
│   ├── fact_transaction/
│   ├── fact_portfolio/
│   ├── dim_customer/
│   ├── dim_account/
│   ├── dim_product/
│   └── daily_portfolio_summary/
│
├── rejected/
│   └── <entity>/
│
├── quarantine/
│   ├── null/
│   ├── duplicate/
│   ├── schema/
│   ├── datatype/
│   ├── business_rule/
│   └── referential_integrity/
│
├── archive/
│
├── audit/
│   ├── pipeline_runs/
│   ├── data_quality/
│   └── reconciliation/
│
└── scripts/
```

---

# 📁 Partition Strategy

For transaction-related data:

```text
year=2026/
month=10/
day=06/
```

Example:

```text
silver/transaction/
    year=2026/
        month=10/
            day=06/
```

Partitioning will be implemented using PySpark:

```python
df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)
```

### Important

Date partitions will **not be manually created**.

Spark will create them during the write operation.

---

# 🥉 Bronze Layer

The Bronze layer stores data close to the source format after basic ingestion.

Typical responsibilities:

- Preserve source data
- Add ingestion metadata
- Add ingestion timestamp
- Track source file
- Track pipeline run ID
- Perform basic schema handling
- Convert raw data to an analytics-friendly format where appropriate

Example metadata:

```text
source_file
source_system
ingestion_timestamp
pipeline_run_id
```

---

# 🥈 Silver Layer

Silver contains cleaned and transformed data.

Typical transformations:

- Trim strings
- Rename columns
- Cast data types
- Standardize dates
- Standardize uppercase/lowercase
- Remove duplicates
- Handle NULL values
- Apply business rules
- Join datasets
- Calculate derived columns
- CDC processing
- SCD Type 2 processing
- Referential integrity validation

Example:

```text
amount = quantity * price
```

---

# 🥇 Gold Layer

Gold contains business-ready datasets.

The POC follows a **Star Schema**.

## Dimension Tables

### dim_customer

```text
customer_sk
customer_id
customer_name
email
city
state
country
effective_date
expiry_date
is_current
```

### dim_account

```text
account_sk
account_id
customer_sk
account_type
account_status
effective_date
expiry_date
is_current
```

### dim_product

```text
product_sk
product_id
product_name
product_type
risk_category
effective_date
expiry_date
is_current
```

---

# Fact Tables

## fact_transaction

```text
transaction_sk
transaction_id
customer_sk
account_sk
product_sk
transaction_date
transaction_type
quantity
price
amount
```

Example relationship:

```text
                dim_customer
                     │
                     │
                     ▼
dim_product ─── fact_transaction ─── dim_account
```

---

# 🔄 Data Validation Strategy

Validation is performed at multiple stages.

## 1. Pre-Validation

Location:

```text
S3 Landing → Lambda
```

Checks:

- File exists
- File is not empty
- File extension
- Filename convention
- Duplicate file
- Basic header validation
- Required columns

If validation fails:

```text
landing → rejected/
```

---

## 2. Data-Level Validation

Location:

```text
Bronze → Glue/EMR
```

Checks:

- NULL values
- Duplicate records
- Schema mismatch
- Data type validation
- Invalid dates
- Negative values
- Referential integrity
- Business rules

Invalid records:

```text
bronze → quarantine/
```

---

## 3. Post-Transformation Validation

After transformation:

```text
Bronze
   ↓
Transformation
   ↓
Post-Transformation Validation
   ↓
Silver
```

Checks:

- Row count
- NULL checks
- Duplicate checks
- Business rules
- Referential integrity
- Transformation correctness
- Source-to-target reconciliation

---

## 4. Post-Load Validation

After loading Gold:

```text
Silver
   ↓
Gold
   ↓
Post-Load Validation
   ↓
Athena
```

Checks:

- Source vs target row count
- SUM reconciliation
- Record existence
- Partition availability
- Data freshness
- Sample record validation

---

# 🔁 Incremental Processing

The POC will implement incremental processing using:

### AWS Glue Bookmarks

Used to track processed data for supported Glue job inputs.

### Watermark

Example:

```text
last_processed_timestamp
```

Pipeline processes:

```text
WHERE updated_at > last_watermark
```

### Bookmark vs Watermark

| Glue Bookmark | Watermark |
|---|---|
| AWS Glue feature | Application-controlled logic |
| Tracks processed input | Tracks business/data timestamp |
| Useful for file-based incremental processing | Useful for timestamp/ID based incremental processing |
| Managed by Glue | Stored in metadata/control table |

Both approaches will be tested in the POC.

---

# 🔄 CDC

The project will simulate:

```text
INSERT
UPDATE
DELETE
```

Example:

```text
customer_id = 101
```

Original:

```text
customer_name = John
city = Hyderabad
```

Updated:

```text
customer_name = John
city = Mumbai
```

The pipeline identifies the changed record using keys and change information.

---

# ♻️ SCD Type 2

Customer history will be maintained using:

```text
customer_sk
customer_id
effective_date
expiry_date
is_current
```

Example:

```text
customer_sk | customer_id | city       | effective_date | expiry_date | is_current
--------------------------------------------------------------------------------
1           | 101         | Hyderabad  | 2026-01-01     | 2026-10-05  | N
2           | 101         | Mumbai     | 2026-10-06     | 9999-12-31  | Y
```

This allows historical customer information to be preserved.

---

# 🚨 Error Scenarios

This POC intentionally generates bad data.

## Data Quality Scenarios

```text
01_duplicates
02_nulls
03_schema_drift
04_data_type_change
05_bad_dates
06_negative_values
07_referential_integrity
```

## Spark / Data Distribution Scenarios

```text
08_skew
09_hot_key
10_multiple_hot_keys
11_null_key_skew
12_low_cardinality
13_wrong_partitioning
14_small_files
```

## Incremental / Processing Scenarios

```text
15_late_arriving
16_out_of_order
17_cdc
18_scd2
```

## File-Level Scenarios

```text
19_corrupt_files
20_empty_files
21_large_volume
```

---

# 🔥 Production Failure Scenarios

The POC will also simulate infrastructure and pipeline failures.

### AWS Glue

- Glue job failure
- Glue timeout
- Schema mismatch
- Crawler failure
- Bookmark issue

### EMR

- Spark OOM
- Executor failure
- Shuffle explosion
- Skew
- Wrong partitioning
- Resource exhaustion
- Job timeout

### Lambda

- Lambda timeout
- Lambda retry
- Duplicate S3 event
- Permission failure

### Step Functions

- Task failure
- Retry
- Timeout
- Partial pipeline failure
- Duplicate pipeline execution

### S3

- AccessDenied
- Missing file
- Empty file
- Corrupt file
- Duplicate file

### Athena

- Table not found
- Missing partitions
- Too much data scanned
- Incorrect schema

### SQS/SNS

- Message failure
- Retry
- Dead Letter Queue
- Notification failure

### IAM

- AccessDenied
- Missing permissions
- Least privilege validation

---

# 🧪 Failure Testing Methodology

Every production issue will follow the same process:

```text
1. Generate bad data
        ↓
2. Run pipeline
        ↓
3. Observe failure
        ↓
4. Check CloudWatch / Spark logs
        ↓
5. Identify root cause
        ↓
6. Implement fix
        ↓
7. Rerun pipeline
        ↓
8. Validate output
        ↓
9. Document solution
        ↓
10. Prepare interview explanation
```

---

# 📊 Monitoring

CloudWatch will be used for:

- Lambda logs
- Glue logs
- EMR logs
- Pipeline failures
- Processing duration
- Error counts
- Data quality failures

Example metrics:

```text
records_processed
records_failed
records_quarantined
records_loaded
pipeline_duration
pipeline_status
data_freshness
```

---

# 🔔 Alerting

SNS will be used for pipeline notifications.

Example:

```text
Pipeline SUCCESS
Pipeline FAILED
Data Quality FAILED
SLA BREACHED
```

SQS/DLQ will be used for messages that cannot be successfully processed.

---

# 📝 Audit Logging

DynamoDB / S3 audit data will maintain:

```text
pipeline_run_id
pipeline_name
source
entity
start_time
end_time
status
records_read
records_processed
records_failed
records_loaded
error_message
```

Example:

```text
RUN-20261006-001
```

---

# 🔐 Security

The POC will implement:

- IAM least privilege
- S3 Block Public Access
- S3 encryption
- IAM roles for Glue/EMR/Lambda
- No hard-coded credentials
- Secrets stored outside source code
- Optional AWS KMS encryption
- Lake Formation governance

---

# 💰 S3 Optimization

The POC will test:

### File Format

```text
CSV / JSON
      ↓
Parquet
      ↓
Snappy compression
```

### Partitioning

Good:

```text
year/month/day
```

Bad:

```text
customer_id
transaction_id
```

High-cardinality columns can create too many partitions.

---

# ⚡ Spark Optimization

The project will demonstrate:

### Repartition

Used when increasing or redistributing partitions.

```python
df.repartition(20)
```

### Coalesce

Used when reducing partitions without a full shuffle.

```python
df.coalesce(5)
```

### Broadcast Join

For small dimension tables:

```python
from pyspark.sql.functions import broadcast

df.join(
    broadcast(dim_df),
    "customer_id"
)
```

### Caching

Used only when the same DataFrame is reused multiple times.

```python
df.cache()
```

---

# 📦 Small File Problem

The POC will intentionally generate many small files.

Example:

```text
100,000 files × 10 KB
```

Then optimize them into larger Parquet files.

This will demonstrate:

```text
Small Files
     ↓
Too many S3 objects
     ↓
More metadata operations
     ↓
Poor Spark performance
     ↓
Higher processing overhead
```

---

# 🏗️ Infrastructure as Code

Terraform will be used to provision infrastructure such as:

```text
S3
IAM
Lambda
Glue
DynamoDB
SNS
SQS
CloudWatch
Step Functions
```

Example structure:

```text
terraform/
│
├── provider.tf
├── variables.tf
├── outputs.tf
├── s3.tf
├── iam.tf
├── lambda.tf
├── glue.tf
├── dynamodb.tf
├── sns.tf
├── sqs.tf
├── stepfunctions.tf
└── cloudwatch.tf
```

---

# 🔄 CI/CD

The project will use GitHub/Jenkins for deployment.

Example:

```text
Developer
   ↓
Git
   ↓
GitHub
   ↓
CI/CD
   ↓
Terraform
   ↓
AWS Infrastructure
   ↓
Deploy PySpark / Lambda Code
```

---

# 📁 Proposed GitHub Repository

```text
aws-financial-data-engineering-poc/
│
├── README.md
│
├── data/
│   ├── good/
│   ├── duplicates/
│   ├── nulls/
│   ├── schema_drift/
│   ├── datatype_change/
│   ├── bad_dates/
│   ├── negative_values/
│   ├── referential_integrity/
│   ├── skew/
│   ├── hot_key/
│   ├── multiple_hot_keys/
│   ├── null_key_skew/
│   ├── low_cardinality/
│   ├── wrong_partitioning/
│   ├── small_files/
│   ├── late_arriving/
│   ├── out_of_order/
│   ├── cdc/
│   ├── scd2/
│   ├── corrupt_files/
│   ├── empty_files/
│   └── large_volume/
│
├── lambda/
│   └── pre_validation/
│
├── glue/
│   ├── ingestion/
│   ├── validation/
│   ├── transformation/
│   ├── scd2/
│   ├── cdc/
│   └── reconciliation/
│
├── emr/
│   ├── jobs/
│   ├── optimization/
│   └── failure_scenarios/
│
├── sql/
│   ├── staging/
│   ├── dimensions/
│   ├── facts/
│   └── validation/
│
├── step_functions/
│   └── state_machine.json
│
├── terraform/
│   ├── provider.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── s3.tf
│   ├── iam.tf
│   ├── lambda.tf
│   ├── glue.tf
│   ├── emr.tf
│   ├── dynamodb.tf
│   ├── sns.tf
│   ├── sqs.tf
│   ├── stepfunctions.tf
│   └── cloudwatch.tf
│
├── tests/
│   ├── data_quality/
│   ├── integration/
│   └── failure_scenarios/
│
└── docs/
    ├── architecture/
    ├── data_dictionary/
    ├── failure_scenarios/
    └── interview_notes/
```

---

# 🚀 Initial S3 Setup

AWS CLI is being used from **Windows CMD**.

## 1. Verify AWS CLI

```cmd
aws sts get-caller-identity
```

---

## 2. Set AWS Region

```cmd
aws configure set region ap-south-1
```

Verify:

```cmd
aws configure get region
```

---

## 3. Set Bucket Name

```cmd
set BUCKET=financial-data-engineering-poc-bhargav-2026
```

Verify:

```cmd
echo %BUCKET%
```

---

# 🪣 Create S3 Bucket

Because the region is Mumbai:

```cmd
aws s3api create-bucket --bucket %BUCKET% --region ap-south-1 --create-bucket-configuration LocationConstraint=ap-south-1
```

Verify:

```cmd
aws s3 ls
```

---

# 🔐 Enable Versioning

```cmd
aws s3api put-bucket-versioning --bucket %BUCKET% --versioning-configuration Status=Enabled
```

Verify:

```cmd
aws s3api get-bucket-versioning --bucket %BUCKET%
```

Expected:

```text
Status: Enabled
```

---

# 🔒 Enable S3 Encryption

```cmd
aws s3api put-bucket-encryption --bucket %BUCKET% --server-side-encryption-configuration "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"AES256\"}}]}"
```

---

# 🚫 Block Public Access

```cmd
aws s3api put-public-access-block --bucket %BUCKET% --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

---

# 📂 Create Main S3 Prefixes

```cmd
aws s3api put-object --bucket %BUCKET% --key landing/
aws s3api put-object --bucket %BUCKET% --key bronze/
aws s3api put-object --bucket %BUCKET% --key silver/
aws s3api put-object --bucket %BUCKET% --key gold/
aws s3api put-object --bucket %BUCKET% --key rejected/
aws s3api put-object --bucket %BUCKET% --key quarantine/
aws s3api put-object --bucket %BUCKET% --key archive/
aws s3api put-object --bucket %BUCKET% --key audit/
aws s3api put-object --bucket %BUCKET% --key scripts/
```

---

# 🔍 Verify S3 Structure

```cmd
aws s3 ls s3://%BUCKET%/
```

Recursive:

```cmd
aws s3 ls s3://%BUCKET%/ --recursive
```

---

# 🎯 Project Goals

By completing this POC, the following real-world Data Engineering concepts will be demonstrated:

- AWS Data Lake
- Medallion Architecture
- S3
- Lambda
- Glue
- EMR
- PySpark
- Athena
- QuickSight
- Step Functions
- EventBridge
- CDC
- SCD Type 2
- Incremental Processing
- Glue Bookmarks
- Watermarks
- Data Quality
- Data Validation
- Data Reconciliation
- Error Handling
- SQS/DLQ
- SNS
- CloudWatch
- DynamoDB Audit
- IAM
- Terraform
- CI/CD
- Spark Optimization
- Data Skew
- Partition Optimization
- Small File Optimization
- Production Troubleshooting

---

# 🧑‍💻 Learning Approach

This is not only a **happy-path pipeline**.

The project will intentionally break the pipeline and solve real production problems.

For every issue:

```text
CREATE ISSUE
     ↓
RUN PIPELINE
     ↓
OBSERVE FAILURE
     ↓
CHECK LOGS
     ↓
FIND ROOT CAUSE
     ↓
FIX
     ↓
RERUN
     ↓
VALIDATE
     ↓
DOCUMENT
     ↓
INTERVIEW ANSWER
```

This approach is intended to build both:

**Hands-on AWS Data Engineering skills + Production troubleshooting skills.**

---

# 📌 Project Status

### Phase 1 — Architecture
- [x] Architecture designed
- [x] S3 structure designed
- [x] Bronze/Silver/Gold defined
- [x] Star schema designed
- [x] Validation strategy defined
- [x] Failure scenarios defined

### Phase 2 — AWS Infrastructure
- [x] AWS CLI configured
- [x] S3 bucket creation
- [x] Versioning
- [x] Encryption
- [x] Public access blocking
- [ ] IAM
- [ ] Lambda
- [ ] Glue
- [ ] EMR
- [ ] Step Functions
- [ ] SNS
- [ ] SQS/DLQ
- [ ] DynamoDB
- [ ] CloudWatch

### Phase 3 — Data
- [ ] Generate clean datasets
- [ ] Generate bad datasets
- [ ] Upload datasets
- [ ] Test ingestion

### Phase 4 — Processing
- [ ] Bronze ingestion
- [ ] Data validation
- [ ] Transformations
- [ ] Silver
- [ ] CDC
- [ ] SCD2
- [ ] Gold

### Phase 5 — Analytics
- [ ] Athena
- [ ] QuickSight
- [ ] Reconciliation
- [ ] Data freshness

### Phase 6 — Production Scenarios
- [ ] Skew
- [ ] Hot keys
- [ ] Small files
- [ ] OOM
- [ ] Schema drift
- [ ] Duplicate files
- [ ] Late-arriving data
- [ ] Pipeline retry
- [ ] Partial failure
- [ ] Idempotency

### Phase 7 — DevOps
- [ ] Terraform
- [ ] CI/CD
- [ ] GitHub Actions/Jenkins

---

# 📚 Documentation

Each major production scenario will have its own documentation:

```text
docs/failure_scenarios/
```

Example:

```text
docs/failure_scenarios/
├── spark_oom.md
├── data_skew.md
├── small_files.md
├── schema_drift.md
├── late_arriving_data.md
├── duplicate_files.md
├── glue_failure.md
├── lambda_timeout.md
└── pipeline_retry.md
```

Each document will contain:

```text
Problem
↓
Why it happened
↓
How to reproduce
↓
Logs / symptoms
↓
Root cause
↓
Solution
↓
Optimization
↓
Interview explanation
```

---

# ⭐ Final Outcome

The final project will represent a production-style AWS Financial Data Lake:

```text
Sources
   ↓
S3 Landing
   ↓
Lambda
   ↓
Pre-Validation
   ↓
Bronze
   ↓
Glue / EMR
   ↓
Data Validation
   ↓
Transformation
   ↓
Post-Transformation Validation
   ↓
Silver
   ↓
CDC / SCD2
   ↓
Gold
   ↓
Post-Load Validation
   ↓
Athena
   ↓
QuickSight
```

With:

```text
Monitoring
+
Alerting
+
Audit
+
Error Handling
+
DLQ
+
Security
+
Terraform
+
CI/CD
+
Performance Optimization
```

---

## 👨‍💻 Author

**Dundu Bhargav**

AWS Data Engineer | PySpark | SQL | AWS | Data Lake | ETL

---

## ⭐ Purpose

This repository is created as a **hands-on AWS Data Engineering learning and interview preparation project**, focusing on real-world production scenarios rather than only theoretical examples.
