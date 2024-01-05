# RappiDriverChain: Roda's Data Pipeline for Rappi Driver Data

## Introduction
This repository contains the implementation of Roda's advanced data pipeline system, designed to handle GPS and operational data from Rappi drivers. It's engineered for efficient data collection, processing, and maintaining data and operational integrity.

## System Architecture
The architecture utilizes AWS services to automate data extraction, transformation, storage, and monitoring. It includes AWS Lambda, Amazon S3, and Step Functions for managing parallel processing workflows.

### Dataset Clarification
- The datasets, Guajira and Roda, are associated with devices in Bogota.
- Guajira dataset pertains to bicycles, while Roda dataset pertains to motorbikes.

## Deployment

### AWS Infrastructure and Terraform
To deploy the AWS infrastructure:
1. Navigate to the `terraform` directory.
2. Run `terraform apply` to deploy or update the infrastructure.
    * This process also deploys a table named `routes` under the `rappi_driver_db` database in the AWS Glue Catalog, as part of the automated infrastructure setup. The `routes` table is populated from data stored at `s3://rodaapp-rappidriverchain/rappi_driver_routes/`, which contains the processed data.

### Updating Lambda Functions and Docker Images
To update Lambda functions and Docker images:
1. Return to the root directory of the repository.
2. Run `make deploy_tribu_datapipeline`. This command performs the following actions:
   - Builds and uploads new Docker images for the extracting and processing scripts to AWS Elastic Container Registry (ECR).
   - Updates the AWS Lambda functions to use the newly uploaded Docker images, ensuring they run the latest version of the scripts.

## Running a Backfill for Tribu Data Pipeline
To run a backfill for a specific date:
1. Navigate to `tribu_datapipeline`.
2. Execute `make backfill_extracting DATASET_TYPE=guajira DATE=YYYY-MM-DD`.
3. Execute `make backfill_processing DATASET_TYPE=guajira DATE=YYYY-MM-DD`.
   - This approach allows for partial backfills, enabling the extraction and processing steps to be executed separately as needed.

## Data Storage and Structure
### S3 Data Storage
- Raw data: `s3://rodaapp-rappidriverchain/tribu_data/`
- Processed data: `s3://rodaapp-rappidriverchain/rappi_driver_routes/`

### Data Handling
- `extracting` pushes data to the `tribu_data` bucket prefix.
- `processing` stores transformed data in the `rappi_driver_routes` bucket prefix.

## Data Querying and Analysis Tools

### AWS Glue Catalog - `routes` Table
- **Database**: `rappi_driver_db`
- **Table**: `routes`
- **Data Source**: The `routes` table is populated from data stored at `s3://rodaapp-rappidriverchain/rappi_driver_routes/`, which contains the processed data.
- **Usage**: Accessible through Amazon Athena and Trino Visual Editor for advanced data analytics and visualization.
