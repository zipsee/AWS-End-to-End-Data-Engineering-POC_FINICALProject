```markdown
# AWS Financial Data Engineering POC

## Overview

This project is an end-to-end AWS Data Engineering POC for a financial data platform.

The main objective was to build and validate an event-driven data pipeline using AWS services for data ingestion, validation, transformation, orchestration, cataloging, and analytics.

The infrastructure was **initially created and tested using AWS CLI**. After validating the architecture, the infrastructure was converted into **Terraform Infrastructure as Code (IaC)** so that the environment can be recreated consistently and maintained through Git/GitHub.

---

## Architecture

```text
                         SOURCE FILES
                              |
                              v
                     +----------------+
                     |   S3 LANDING   |
                     +-------+--------+
                             |
                       S3 ObjectCreated
                             |
                             v
                     +----------------+
                     |     Lambda     |
                     |  S3 Trigger   |
                     +-------+--------+
                             |
                             v
                  +-----------------------+
                  |    Step Functions     |
                  |     Orchestration     |
                  +----------+------------+
                             |
                             v
                  +-----------------------+
                  | Glue Bronze -> Silver |
                  |       PySpark         |
                  +----------+------------+
                             |
                             v
                     +---------------+
                     |    SILVER     |
                     | Validated Data|
                     +-------+-------+
                             |
                             v
                  +-----------------------+
                  | Glue Silver -> Gold   |
                  |       PySpark         |
                  +----------+------------+
                             |
                             v
                     +---------------+
                     |     GOLD      |
                     | Fact + Dims   |
                     +-------+-------+
                             |
                             v
                     +---------------+
                     | Glue Crawler  |
                     | Data Catalog   |
                     +-------+-------+
                             |
                             v
                     +---------------+
                     |    Athena     |
                     |  SQL Queries  |
                     +---------------+
```

---

## AWS Services Used

| AWS Service | Purpose |
|---|---|
| Amazon S3 | Data Lake storage |
| AWS Lambda | S3 event trigger |
| AWS Glue | PySpark ETL and data validation |
| AWS Step Functions | Pipeline orchestration |
| AWS Glue Crawler | Catalog Gold datasets |
| AWS Glue Data Catalog | Metadata management |
| Amazon Athena | SQL analytics |
| AWS IAM | Roles and permissions |
| Amazon CloudWatch | Logs and monitoring |
| AWS CLI | Initial infrastructure creation and testing |
| Terraform | Infrastructure as Code and reproducible deployment |

---

# S3 Data Lake

The S3 data lake is organized into multiple processing layers.

```text
financial-data-engineering-poc/
|
+-- landing/
|   +-- customer/
|   +-- account/
|   +-- product/
|   +-- transaction/
|   +-- portfolio/
|
+-- bronze/
|   +-- customer/
|   +-- account/
|   +-- product/
|   +-- transaction/
|   +-- portfolio/
|
+-- silver/
|   +-- customer/
|   +-- account/
|   +-- product/
|   +-- transaction/
|   +-- portfolio/
|
+-- gold/
|   +-- dim_customer/
|   +-- dim_account/
|   +-- dim_product/
|   +-- dim_portfolio/
|   +-- dim_date/
|   +-- fact_transaction/
|   +-- daily_portfolio_summary/
|
+-- quarantine/
|   +-- null/
|   +-- duplicate/
|   +-- schema/
|   +-- datatype/
|   +-- business_rule/
|
+-- rejected/
|   +-- customer/
|
+-- audit/
|   +-- pipeline_runs/
|   +-- data_quality/
|
+-- archive/
|
+-- scripts/
```

---

# Bronze Layer

The Bronze layer contains source-oriented data used for downstream processing.

The Bronze to Silver Glue job supports:

- Customer
- Account
- Product
- Transaction
- Portfolio

The processing includes:

- Schema validation
- Required column validation
- Data type validation
- NULL validation
- Duplicate validation
- Primary key validation
- Business rule validation
- Quarantine handling
- Audit output
- Transformation to Parquet

---

# Silver Layer

The Silver layer contains validated and transformed data.

The processing flow is:

```text
Raw / Bronze Data
       |
       v
Schema Validation
       |
       v
Data Type Validation
       |
       v
NULL Validation
       |
       v
Duplicate Validation
       |
       v
Business Rule Validation
       |
       v
Transformation
       |
       v
Silver Parquet
```

Processed data is stored in Parquet format.

---

# Gold Layer

The Gold layer follows a star-schema design.

## Dimensions

```text
dim_customer
dim_account
dim_product
dim_portfolio
dim_date
```

## Fact

```text
fact_transaction
```

The intended model is:

```text
                   dim_customer
                        |
                        |
dim_product ---- fact_transaction ---- dim_account
                        |
                        |
                     dim_date
```

---

# Fact Transaction

The central transaction fact contains:

```text
transaction_id
customer_id
account_id
product_id
transaction_date
transaction_type
quantity
price
amount
currency
status
updated_at
```

The fact table is designed for transaction-level financial analytics.

---

# Data Quality

Data quality checks are performed during the Bronze to Silver processing.

```text
                 Input Data
                     |
                     v
              Schema Validation
                     |
                     v
             Data Type Validation
                     |
                     v
               NULL Validation
                     |
                     v
            Duplicate Validation
                     |
                     v
            Primary Key Validation
                     |
                     v
           Business Rule Validation
                     |
              +------+------+
              |             |
            Valid         Invalid
              |             |
              v             v
           Silver       Quarantine
```

Invalid records are separated into quarantine locations:

```text
quarantine/
|
+-- null/
+-- duplicate/
+-- schema/
+-- datatype/
+-- business_rule/
```

---

# Event-Driven Pipeline

The pipeline is triggered when a new object is created in the S3 landing area.

```text
File Upload
     |
     v
S3 ObjectCreated Event
     |
     v
Lambda
     |
     v
Step Functions
     |
     v
Glue Bronze -> Silver
     |
     v
Glue Silver -> Gold
```

The S3 event notification is configured for the:

```text
landing/
```

prefix.

---

# Step Functions

Step Functions is used to orchestrate the ETL process.

```text
Start
  |
  v
Bronze -> Silver
  |
  v
Silver -> Gold
  |
  v
Success
```

The workflow also contains failure handling.

```text
Glue Failure
     |
     v
Catch
     |
     v
Pipeline Failed
```

---

# AWS CLI Implementation

The initial AWS infrastructure was created and configured using **AWS CLI**.

This included:

```text
S3
IAM
Lambda
Glue
Glue Crawler
Step Functions
Athena
S3 Event Notification
CloudWatch
```

Examples of AWS CLI operations used during the project:

```bash
aws sts get-caller-identity

aws configure set region ap-south-1

aws s3api create-bucket ...

aws s3api put-bucket-versioning ...

aws s3api put-bucket-encryption ...

aws iam create-role ...

aws iam put-role-policy ...

aws glue create-job ...

aws glue create-crawler ...

aws lambda create-function ...

aws stepfunctions create-state-machine ...

aws athena create-work-group ...
```

The infrastructure was created and tested manually before being converted to Terraform.

---

# Testing Performed

## Customer Pipeline

Customer data was tested through the validation and transformation pipeline.

Testing included:

- Valid records
- Invalid records
- Data quality validation
- Silver output
- Quarantine/rejected data handling

---

## Transaction Pipeline

A transaction file was uploaded into the S3 landing location.

The upload triggered:

```text
S3
 |
 v
Lambda
 |
 v
Step Functions
 |
 v
Glue Bronze -> Silver
 |
 v
Glue Silver -> Gold
```

The Step Functions execution completed successfully.

Transaction data was generated in the Silver layer and then written to the Gold:

```text
gold/fact_transaction/
```

A customer dimension was also generated in:

```text
gold/dim_customer/
```

---

# Troubleshooting

During testing, Athena initially returned:

```text
TABLE_NOT_FOUND:
Table 'awsdatacatalog.financial_poc.fact_transaction' does not exist
```

## Root Cause

The Glue crawler was initially configured only for:

```text
gold/dim_customer/
```

Therefore, the `fact_transaction` dataset was not registered in the Glue Data Catalog.

## Fix

The crawler target was changed to:

```text
gold/
```

The crawler was executed again.

After the crawler completed, the Data Catalog contained:

```text
dim_customer
fact_transaction
gold
```

This validated the Gold cataloging process and demonstrated real pipeline troubleshooting.

---

# Terraform Infrastructure as Code

After the AWS CLI implementation was validated, the infrastructure was converted into Terraform.

Terraform is used as the **Infrastructure as Code implementation**, not simply as a backup.

The Terraform configuration defines the core infrastructure required to recreate the pipeline.

```text
S3
IAM
Lambda
Glue
Glue Crawler
Step Functions
Athena
S3 Event Notification
```

The workflow is:

```text
AWS CLI
   |
   v
Build Infrastructure
   |
   v
Test Pipeline
   |
   v
Troubleshoot
   |
   v
Validate Architecture
   |
   v
Terraform
   |
   v
Infrastructure as Code
   |
   v
Git / GitHub
```

---

# Terraform Structure

```text
terraform/
|
+-- provider.tf
+-- variables.tf
+-- outputs.tf
+-- s3.tf
+-- iam.tf
+-- lambda.tf
+-- glue.tf
+-- stepfunctions.tf
+-- athena.tf
|
+-- glue/
|   +-- glue_bronze_to_silver.py
|   +-- glue_silver_to_gold.py
|
+-- lambda/
    +-- s3_stepfunctions_trigger.py
```

---

# Terraform Commands

Initialize Terraform:

```bash
terraform init
```

Validate the configuration:

```bash
terraform validate
```

Review infrastructure changes:

```bash
terraform plan
```

Deploy:

```bash
terraform apply
```

Destroy the Terraform-managed infrastructure:

```bash
terraform destroy
```

The Terraform configuration was validated successfully and the planned infrastructure contained:

```text
Plan: 54 to add, 0 to change, 0 to destroy.
```

---

# Security

The project uses IAM roles for AWS services.

Security-related configurations include:

- IAM roles
- IAM policies
- S3 encryption
- S3 versioning
- S3 public access controls
- Service-specific permissions
- No AWS credentials stored in the repository

AWS credentials are kept outside the Git repository.

---

# Data Format

Source files can be provided as structured data files such as CSV.

Processed data is stored in Parquet format.

```text
Source File
     |
     v
Validation
     |
     v
Transformation
     |
     v
Parquet
     |
     v
Silver / Gold
```

---

# Current Implemented Scope

The core implemented and tested architecture is:

```text
S3 Landing
    |
    v
S3 ObjectCreated
    |
    v
Lambda
    |
    v
Step Functions
    |
    v
Glue Bronze -> Silver
    |
    v
Glue Silver -> Gold
    |
    v
Glue Crawler
    |
    v
Glue Data Catalog
    |
    v
Athena
```

The customer and transaction flows were used for testing and validation.

---

# Current Limitations

The following items are planned enhancements and are not represented as fully implemented production features in the current POC:

- Full CDC implementation
- SCD Type 2
- Advanced incremental processing
- DynamoDB audit framework
- SNS alerting
- SQS / DLQ
- Lake Formation
- EMR processing
- CI/CD pipeline
- Automated QuickSight deployment
- Advanced Spark optimization
- Full production monitoring
- Complete daily batch marker / manifest implementation

---

# Future Enhancements

Planned improvements include:

1. Incremental processing
2. Glue bookmarks
3. Watermark processing
4. CDC
5. SCD Type 2
6. Referential integrity checks
7. Reconciliation framework
8. DynamoDB pipeline audit
9. SNS notifications
10. SQS / DLQ
11. CloudWatch dashboards
12. QuickSight dashboard
13. Spark performance optimization
14. Small-file optimization
15. Partition optimization
16. GitHub Actions CI/CD
17. Automated Terraform deployment

---

# Project Flow

The project follows a practical Data Engineering workflow:

```text
Design
  |
  v
Build with AWS CLI
  |
  v
Test
  |
  v
Find Failure
  |
  v
Investigate Root Cause
  |
  v
Fix
  |
  v
Rerun
  |
  v
Validate
  |
  v
Convert to Terraform
  |
  v
Store Infrastructure as Code in GitHub
```

---

# Interview Summary

### What did you build?

I built an event-driven financial data pipeline on AWS using S3, Lambda,
Step Functions, Glue, Glue Data Catalog and Athena.

### How did you build it?

I initially created and configured the AWS infrastructure using AWS CLI.
This helped me understand how each AWS service was configured and how the
services interacted.

### How did you validate it?

I uploaded test data, triggered the pipeline through S3 events, validated
Step Functions executions, checked Glue processing, verified Silver and
Gold outputs, and cataloged the Gold data using Glue Crawler.

### Why Terraform?

After validating the architecture using AWS CLI, I converted the
infrastructure into Terraform so it could be recreated consistently and
managed as Infrastructure as Code through Git/GitHub.

### What did you troubleshoot?

I encountered an Athena `TABLE_NOT_FOUND` error because the Glue crawler
was only targeting the customer dimension path. I changed the crawler
target to the complete Gold location, reran the crawler, and verified that
`fact_transaction` was successfully registered in the Data Catalog.

---

# Key Skills Demonstrated

```text
AWS
AWS CLI
Amazon S3
AWS Lambda
AWS Glue
PySpark
AWS Step Functions
Glue Data Catalog
AWS Athena
IAM
CloudWatch
Terraform
Infrastructure as Code
Data Lake
ETL
Data Quality
Data Validation
Parquet
Star Schema
Fact / Dimension Modelling
Event-Driven Architecture
Pipeline Orchestration
Troubleshooting
Git / GitHub
```

---

# Final Architecture

```text
                         SOURCE DATA
                              |
                              v
                       +-------------+
                       | S3 LANDING  |
                       +------+------+
                              |
                       ObjectCreated
                              |
                              v
                       +-------------+
                       |   LAMBDA    |
                       +------+------+
                              |
                              v
                    +-------------------+
                    |  STEP FUNCTIONS   |
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    | BRONZE -> SILVER  |
                    |      GLUE         |
                    +---------+---------+
                              |
                              v
                         +--------+
                         | SILVER |
                         +---+----+
                             |
                             v
                    +-------------------+
                    | SILVER -> GOLD    |
                    |      GLUE         |
                    +---------+---------+
                              |
                              v
                         +--------+
                         |  GOLD  |
                         +---+----+
                             |
                             v
                       +-----------+
                       |   GLUE    |
                       |  CRAWLER  |
                       +-----+-----+
                             |
                             v
                       +-----------+
                       |  ATHENA   |
                       +-----------+
```

---

## Conclusion

This project demonstrates an end-to-end AWS Data Engineering workflow,
starting from manual AWS CLI infrastructure creation and validation,
followed by event-driven ETL processing, data quality validation,
orchestration, Gold-layer modelling, cataloging and Athena analytics.

Once the architecture was validated using AWS CLI, the infrastructure was
codified using Terraform to provide a reproducible and version-controlled
Infrastructure as Code implementation suitable for Git/GitHub.
```
