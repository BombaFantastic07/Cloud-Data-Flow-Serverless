# Serverless ETL Pipeline: S3 → Lambda → AWS Glue → Athena

A GitHub-ready starter project demonstrating an event-driven, serverless data pipeline using **Amazon S3, AWS Lambda, AWS Glue, Amazon Athena, and Python**.

> This repository is a runnable project scaffold, not evidence of an already-deployed AWS environment. Deploy and test it in your own AWS account before describing it as deployed.

## Architecture

1. A CSV file is uploaded to the raw S3 bucket.
2. An S3 `ObjectCreated` event invokes the Lambda function.
3. Lambda starts an AWS Glue ETL job and passes the uploaded object path.
4. Glue cleans and transforms the CSV data, then writes Parquet files to the curated S3 prefix.
5. Athena queries the curated data using SQL.

```text
CSV upload
   |
   v
Amazon S3 (raw/)
   |
   | ObjectCreated event
   v
AWS Lambda (Python) ---- starts ----> AWS Glue ETL job
                                         |
                                         | clean / transform
                                         v
                                  Amazon S3 (curated/, Parquet)
                                         |
                                         v
                                  Amazon Athena (SQL)
```

## Repository layout

```text
.
├── lambda/
│   └── handler.py          # Starts Glue when a raw object arrives
├── glue/
│   └── etl_job.py          # Reads CSV, transforms data, writes Parquet
├── athena/
│   └── sample_queries.sql  # Example analytics queries
├── sample-data/
│   └── orders.csv          # Small sample input dataset
├── requirements.txt
├── .gitignore
└── README.md
```

## Example input

The sample file `sample-data/orders.csv` uses this schema:

| order_id | customer_id | order_date | amount | status |
|---|---|---|---:|---|
| 1001 | C001 | 2026-01-05 | 125.50 | completed |
| 1002 | C002 | 2026-01-06 | 89.99 | pending |

The Glue job casts dates and amounts, removes rows without an order ID, normalizes status values, and writes partitioned Parquet output by order year and month.

## Prerequisites

- An AWS account and permissions to create/use S3, Lambda, Glue, IAM, and Athena resources
- AWS CLI configured (`aws configure`)
- Python 3.10+ for local development
- A Glue job role with access to the raw and curated S3 locations and CloudWatch Logs
- A Lambda execution role with permission to call `glue:StartJobRun` and write CloudWatch Logs

## Setup and deployment

### 1. Create S3 locations

Create a raw bucket and a curated bucket (bucket names must be globally unique), for example:

- `YOUR-RAW-BUCKET`
- `YOUR-CURATED-BUCKET`

Use separate buckets or separate prefixes in one bucket. This example assumes separate buckets.

### 2. Upload the Glue script

Upload `glue/etl_job.py` to a location the Glue job role can read, for example:

```bash
aws s3 cp glue/etl_job.py s3://YOUR-SCRIPTS-BUCKET/glue/etl_job.py
```

### 3. Create the Glue job

In AWS Glue, create a Spark job with:
- Script location: `s3://YOUR-SCRIPTS-BUCKET/glue/etl_job.py`
- IAM role: your Glue job role
- Glue version: a currently supported Glue version
- Job parameters:
  - `--RAW_BUCKET` = `YOUR-RAW-BUCKET`
  - `--CURATED_BUCKET` = `YOUR-CURATED-BUCKET`
  - `--CURATED_PREFIX` = `curated/orders/`

The script accepts `--INPUT_KEY` at run time; Lambda supplies this argument for each uploaded object.

### 4. Deploy the Lambda function

Create a Python Lambda function using `lambda/handler.py` as the source and set the handler to:

```text
handler.lambda_handler
```

Set these environment variables:
- `GLUE_JOB_NAME` = the exact name of your Glue job

The Lambda execution role needs `glue:StartJobRun` on that job and CloudWatch Logs permissions. Package and upload the handler from the `lambda/` directory, or use the Lambda console editor for this small example.

### 5. Configure the S3 event notification

On the raw bucket, add an event notification:
- Event type: **All object create events**
- Prefix: `raw/` (optional but recommended)
- Suffix: `.csv`
- Destination: your Lambda function

Grant the S3 bucket permission to invoke the Lambda function through the console workflow. Keep curated output in a different bucket or prefix so the trigger does not loop on its own output.

### 6. Upload sample data

```bash
aws s3 cp sample-data/orders.csv s3://YOUR-RAW-BUCKET/raw/orders.csv
```

Check the Lambda CloudWatch logs, then monitor the Glue job run in AWS Glue.

### 7. Query with Athena

Create a database and table for the curated Parquet output. For a simple first test, use Athena's table creation wizard and point it at:

```text
s3://YOUR-CURATED-BUCKET/curated/orders/
```

The output is partitioned by `order_year` and `order_month`. Configure the table schema to match the transformed columns and run the example queries in `athena/sample_queries.sql`. You can also use an AWS Glue Data Catalog crawler on the curated prefix to infer the schema and partitions.

Athena requires an S3 query-results location configured in the workgroup or query settings.

## Data transformation

The Glue script:
- Reads a CSV object passed by Lambda
- Trims string fields and drops records without `order_id`
- Casts `order_date` to a date and `amount` to a numeric value
- Normalizes `status`
- Adds `order_year` and `order_month` partitions
- Writes Parquet to the curated S3 location

## Operational considerations

- Configure S3 notifications to target only the raw input prefix.
- Add retry handling and a dead-letter destination for production workloads.
- Consider idempotency and duplicate event handling.
- Restrict IAM policies to the specific resources used.
- Enable encryption and appropriate bucket public-access blocks.
- Monitor Lambda and Glue failures with CloudWatch alarms.
- For larger or evolving datasets, consider schema evolution and catalog partition management.

## Security

Do not commit AWS credentials, access keys, account IDs, private data, or `.env` files. Use IAM roles and environment variables for configuration.

## Resume bullet (use only after validating your own implementation)

“Built an event-driven serverless ETL pipeline using Amazon S3, AWS Lambda, AWS Glue, Athena, and Python to automate data ingestion, transformation to partitioned Parquet, and SQL analytics.”

## License

Choose a license before making this repository public. The code is provided as an educational starter project.
