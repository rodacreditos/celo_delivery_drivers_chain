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

## Data Extraction and Processing

### Credentials for Tribu API Access
The extraction script requires credentials to access the Tribu API, which are fetched according to the dataset type:

- For the Guajira dataset: Credentials are retrieved from `s3://rodaapp-rappidriverchain/credentials/tribu_guajira_credentials.json`.
- For the Roda dataset: Credentials are retrieved from `s3://rodaapp-rappidriverchain/credentials/tribu_roda_credentials.json`.

These JSON files should contain a JSON-formatted string with the keys `user` and `password`, as shown in the example below:

```json
{
    "user": "your_username",
    "password": "your_password"
}
```

Ensure that these credentials are up-to-date and have the necessary permissions for API access.

### Transformation Parameters for Data Processing
The processing script extracts transformation parameters from YAML files stored in S3, depending on the dataset type:

- For the Roda dataset: Parameters are fetched from `s3://rodaapp-rappidriverchain/tribu_metadata/transformations_roda.yaml`.
- For the Guajira dataset: Parameters are fetched from `s3://rodaapp-rappidriverchain/tribu_metadata/transformations_guajira.yaml`.

#### YAML File Structure and Requirements
The YAML files should contain the following sections:

**Mandatory Parameters:**
- `input_datetime_format`: The format of datetime fields in the input data.
- `output_datetime_format`: The desired format of datetime fields in the output data.
- `column_rename_map`: A list of key-value pairs, where the key is the original column name, and the value is the new name after transformation. The order in this map determines the order of the columns in the output CSV. Only the columns listed in `column_rename_map` will be included in the output.

  Example:
  ```yaml
  column_rename_map:
    k_dispositivo: gpsID
    o_fecha_inicial: timestampStart
    o_fecha_final: timestampEnd
    f_distancia: measuredDistance
  ```

**Optional Parameters:**
- `duration_filter`: Filters the data based on the duration. Must include at least a min value. The max value is optional.
- `distance_filter`: Filters the data based on distance. Must include at least a min value. The max value is optional.

  Example:
  ```yaml
  duration_filter:
    min: 2
    max: 90 # 1.5 Hours

    distance_filter:
    min: 147
  ```

Ensure these parameters are correctly configured to meet the specific needs of the data processing script.

### Optional `processing_date` Parameter for Scripts
Both the extraction and processing scripts accept an optional parameter named `processing_date`. This parameter is used to specify the date for which the data should be processed. If not provided, the scripts default to processing data for the previous day. This default is computed within the Lambda function.

Example usage:
- When running a backfill or if you need to process data for a specific date, you can pass the `processing_date` parameter to the scripts.

#### Specifying `processing_date` in Backfill
For running a backfill, the `processing_date` can be specified as follows:
1. Navigate to `tribu_datapipeline`.
2. Execute the backfill commands with the `DATE` parameter set to your desired date.

   ```shell
   make backfill_extracting DATASET_TYPE=guajira DATE=YYYY-MM-DD
   make backfill_processing DATASET_TYPE=guajira DATE=YYYY-MM-DD

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
