import boto3
import csv
import io
import os
import urllib.parse
from datetime import datetime

s3 = boto3.client("s3")

BUCKET = os.environ.get(
    "BUCKET_NAME",
    "financial-data-engineering-poc-bhargav-2026"
)

REQUIRED_COLUMNS = {
    "customer": {
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "city",
        "state",
        "country",
        "created_date",
        "updated_at"
    },
    "account": {
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "opening_date",
        "balance",
        "currency",
        "updated_at"
    },
    "product": {
        "product_id",
        "product_name",
        "product_type",
        "risk_category",
        "interest_rate",
        "status",
        "created_date"
    },
    "transaction": {
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
    },
    "portfolio": {
        "portfolio_id",
        "customer_id",
        "account_id",
        "portfolio_name",
        "portfolio_type",
        "total_value",
        "currency",
        "valuation_date",
        "updated_at"
    }
}


def get_entity_from_key(key):
    parts = key.split("/")

    if len(parts) < 3:
        return None

    if parts[0] != "landing":
        return None

    return parts[1]


def validate_file(bucket, key):
    errors = []

    filename = key.split("/")[-1]

    # ---------------------------------------------------------
    # 1. Validate file extension
    # ---------------------------------------------------------
    if not filename.lower().endswith(".csv"):
        errors.append("Invalid file extension. Only CSV files are supported.")

    # ---------------------------------------------------------
    # 2. Extract entity
    # ---------------------------------------------------------
    entity = get_entity_from_key(key)

    if entity is None:
        errors.append("Invalid S3 path. Expected landing/<entity>/<file>.")

        return {
            "valid": False,
            "errors": errors,
            "entity": None
        }

    if entity not in REQUIRED_COLUMNS:
        errors.append(f"Unsupported entity: {entity}")

        return {
            "valid": False,
            "errors": errors,
            "entity": entity
        }

    # ---------------------------------------------------------
    # 3. Get object metadata
    # ---------------------------------------------------------
    response = s3.head_object(
        Bucket=bucket,
        Key=key
    )

    file_size = response.get("ContentLength", 0)

    # ---------------------------------------------------------
    # 4. Check empty file
    # ---------------------------------------------------------
    if file_size == 0:
        errors.append("File is empty.")

        return {
            "valid": False,
            "errors": errors,
            "entity": entity
        }

    # ---------------------------------------------------------
    # 5. Read file
    # ---------------------------------------------------------
    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    content = obj["Body"].read().decode("utf-8-sig")

    if not content.strip():
        errors.append("File contains no data.")

        return {
            "valid": False,
            "errors": errors,
            "entity": entity
        }

    # ---------------------------------------------------------
    # 6. Read CSV header
    # ---------------------------------------------------------
    reader = csv.DictReader(io.StringIO(content))

    actual_columns = set(reader.fieldnames or [])

    required_columns = REQUIRED_COLUMNS[entity]

    missing_columns = required_columns - actual_columns

    if missing_columns:
        errors.append(
            "Missing required columns: "
            + ", ".join(sorted(missing_columns))
        )

    # ---------------------------------------------------------
    # 7. Check duplicate column names
    # ---------------------------------------------------------
    if reader.fieldnames:
        if len(reader.fieldnames) != len(set(reader.fieldnames)):
            errors.append("Duplicate column names found.")

    # ---------------------------------------------------------
    # 8. Basic record existence check
    # ---------------------------------------------------------
    rows = list(reader)

    if len(rows) == 0:
        errors.append("CSV contains header but no data records.")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "entity": entity,
        "file_size": file_size,
        "record_count": len(rows),
        "columns": sorted(actual_columns)
    }


def copy_to_destination(source_key, destination_key):
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={
            "Bucket": BUCKET,
            "Key": source_key
        },
        Key=destination_key
    )


def lambda_handler(event, context):

    pipeline_run_id = datetime.utcnow().strftime(
        "RUN-%Y%m%d-%H%M%S-%f"
    )

    print("=" * 70)
    print("FINANCIAL DATA POC - PRE VALIDATION")
    print("=" * 70)
    print(f"Pipeline Run ID: {pipeline_run_id}")

    for record in event.get("Records", []):

        try:

            bucket = record["s3"]["bucket"]["name"]

            key = urllib.parse.unquote_plus(
                record["s3"]["object"]["key"]
            )

            print(f"Bucket: {bucket}")
            print(f"Object: {key}")

            # -------------------------------------------------
            # Only process landing files
            # -------------------------------------------------
            if not key.startswith("landing/"):
                print("Skipping non-landing object.")
                continue

            validation = validate_file(
                bucket,
                key
            )

            entity = validation.get("entity")

            print(f"Entity: {entity}")
            print(
                f"File Size: "
                f"{validation.get('file_size', 0)} bytes"
            )
            print(
                f"Record Count: "
                f"{validation.get('record_count', 0)}"
            )

            # -------------------------------------------------
            # VALID FILE
            # -------------------------------------------------
            if validation["valid"]:

                filename = key.split("/")[-1]

                bronze_key = (
                    f"bronze/{entity}/{filename}"
                )

                print("VALIDATION STATUS: PASS")
                print(f"Writing to: {bronze_key}")

                copy_to_destination(
                    key,
                    bronze_key
                )

                print(
                    f"SUCCESS: {key} -> {bronze_key}"
                )

            # -------------------------------------------------
            # INVALID FILE
            # -------------------------------------------------
            else:

                filename = key.split("/")[-1]

                rejected_key = (
                    f"rejected/{entity}/{filename}"
                )

                print("VALIDATION STATUS: FAILED")

                for error in validation["errors"]:
                    print(f"ERROR: {error}")

                print(
                    f"Writing rejected file to: "
                    f"{rejected_key}"
                )

                copy_to_destination(
                    key,
                    rejected_key
                )

                print(
                    f"REJECTED: {key} -> {rejected_key}"
                )

        except Exception as e:

            print("PIPELINE ERROR")
            print(str(e))

            raise

    return {
        "statusCode": 200,
        "message": "Pre-validation completed successfully",
        "pipeline_run_id": pipeline_run_id
    }