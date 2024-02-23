# RappiDriverChain: Roda's Data Pipeline for Rappi Driver Data


## Introduction
RappiDriverChain is a sophisticated data pipeline system built for processing GPS routes from Tribu API. These routes, originating from GPS devices installed in e-bikes and motorbikes, are primarily used by Rappi drivers. The key objective of this pipeline is to evaluate the risk associated with providing loans to these drivers, based on metrics like the number of routes per day, aggregated time per day, and aggregated distance traveled per day.

The pipeline leverages Python and Pandas for data processing, web3py for interacting with the Celo blockchain platform, and a robust AWS tech stack for deploying Lambda functions and a Step Function state machine to orchestrate pipeline execution.

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

## AWS Infrastructure Overview

The following AWS services are deployed through the Terraform scripts to support the data pipeline:

### Lambda Functions
- **`extract_tribu_data` and `process_tribu_data`**: These functions handle the extraction and processing of data, respectively. They're deployed as Docker images to AWS ECR.
- **GPS to Celo Map Sync**: This Lambda function connects to the Roda's database (Airtable) to retrieve contacts data and their assigned GPS. It generates a map from GPS to Celo addresses. For contacts without a Celo address, the script randomly generates an address and updates the contact table in Airtable.
- **Blockchain Publisher**: This script takes the output from the processing task and publishes these routes on the Celo blockchain.

### IAM Roles and Policies
- **Lambda Execution Role (`lambda_exec_role`)**: A role for the Lambda functions, allowing them to assume necessary permissions.
- **Lambda S3 Access Policy**: Grants the Lambda functions access to specific S3 operations.
- **Step Function Role (`sfn_role`)** and **CloudWatch Role (`cloudwatch_role`)**: Roles for Step Functions and CloudWatch to perform their respective tasks.

### CloudWatch Event Rule
- **`daily-trigger-at-6-am`**: Triggers the Step Function State Machine daily at 6 AM UTC (1 AM Bogotá/Colombia).

### Step Function State Machine
- **`TribuStateMachine`**: Orchestrates the data processing workflow, running the Lambda functions in parallel for Guajira and Roda datasets.

### Glue Catalog Database and Table
- **Database (`rappi_driver_db`)**: Hosts the `routes` table.
- **Table (`rappi_driver_routes`)**: Stores the processed data and is designed for querying with tools like Amazon Athena.

### Accessing the Services
- Lambda Functions, IAM Roles/Policies, and the CloudWatch Event Rule can be accessed and managed via the AWS Management Console or AWS CLI.
- The Step Function State Machine's execution can be monitored through the AWS Management Console.
- The Glue Catalog Database and Table are accessible for queries through Amazon Athena or other AWS data services.

### Documentation
- For detailed documentation on each service, refer to the AWS official documentation. Here are some starting points:
  - [AWS Lambda](https://docs.aws.amazon.com/lambda/)
  - [IAM](https://docs.aws.amazon.com/IAM/)
  - [Amazon CloudWatch Events](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/)
  - [AWS Step Functions](https://docs.aws.amazon.com/step-functions/)
  - [AWS Glue Catalog](https://docs.aws.amazon.com/glue/)

## DynamoDB Tables for Route Management
The integration of DynamoDB for unique route ID management necessitates the creation of two specific tables:

RouteIDCounter: Manages the atomic counter for generating unique route IDs.
RouteMappingsUpdated: Stores the mappings between old route IDs and their new unique counterparts, with timestamps to track updates.


## AWS Cost Analysis Report

This section provides an overview of the estimated costs for our AWS infrastructure, specifically focusing on Lambda functions for extraction and processing, AWS Step Functions, Athena queries, and the AWS Glue Data Catalog. These estimates are based on current usage patterns and AWS pricing as of April 2023.

### AWS Lambda Functions

###

#### Extraction Lambda Function
- **Usage**: Invoked twice daily, once for roda and once for guajira data.
- **Average Execution Duration**: ~5.8 seconds (roda), ~4.1 seconds (guajira).
- **Memory Allocation**: 128 MB.
- **Estimated Monthly Cost**: Lambda costs - Without Free Tier (monthly): 0.00 USD (According to the AWS price calculator)

#### Processing Lambda Function
- **Usage**: Invoked twice daily, following the extraction process for both data sets.
- **Average Execution Duration**: ~0.42 seconds (roda), ~0.38 seconds (guajira).
- **Memory Allocation**: 128 MB (fully utilized).
- **Estimated Monthly Cost**: Lambda costs - Without Free Tier (monthly): 0.00 USD

### AWS Step Functions
- **Usage**: Orchestrates the execution of Lambda functions daily. It has one step machine with 4 transitions: extracting and processing steps for both roda an guajira datasets.
- **Estimated Daily/Monthly Cost**: Standard Workflows pricing (monthly): 0.00 USD (The AWS Step Functions Free Tier does not automatically expire at the end of your 12 month AWS Free Tier term, and is available to both existing and new AWS customers indefinitely. For example if we run 30 queries in a month )

### AWS Athena Queries
- **Usage**: Run weekly for data analysis.
- **Average Data Scanned per Query**: Based on the file sizes of 63KB (roda) and 8.2KB (guajira).
- **Estimated Monthly Cost**: SQL queries with per query cost (monthly): 0.00 USD (This will keep true for a while, until we have enough data to query then prices could change.)

### AWS Glue Data Catalog
- **Usage**: Maintains metadata for a single table.
- **Cost Consideration**: Likely minimal or covered under the AWS Free Tier if the table is not frequently modified and the number of objects is low.

### Cost Calculation Methodology
- Costs for Lambda functions are estimated based on the number of requests, execution duration, and memory allocation.
- AWS Step Functions costs are calculated per state transition.
- Athena costs are based on the amount of data scanned per query.
- AWS pricing for Lambda, Step Functions, and Athena can be updated using the [AWS Price Calculator](https://calculator.aws/#/).

### Notes
- These cost estimations are approximations. Actual costs may vary based on exact usage, AWS pricing changes, and specific configurations.
- Regular monitoring of AWS usage and costs through the AWS Management Console is advised for accurate tracking and optimization.

---

For more detailed information or updates to this cost analysis, please contact the project's system administrators.

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
- `input_datetime_format`: The format of datetime fields in the input data. It supports python date format codes.
- `output_datetime_format`: The desired format of datetime fields in the output data. It supports python date format codes and aditionaly it also support `unix` as an option, which converts datetime fields into Unix timestamp format (seconds since January 1, 1970).
- `column_rename_map`: A list of key-value pairs, where the key is the original column name, and the value is the new name after transformation. The order in this map determines the order of the columns in the output CSV. Only the columns listed in `column_rename_map` will be included in the output.

  Example:
  ```yaml
  column_rename_map:
    newID: routeID
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

- `distance_fix`: When this parameter is defined, it will correct the distance values in the dataset. Must define `expected_max_per_hour`, which is the maximum distance expected to be traveled by a vehicule within one hour.

  Example:
  ```yaml
  distance_fix:
    expected_max_per_hour: 60000 # It is expected that as maximum a motorbike trips for 60km in one hour in Bogota urban zone
  ```

- `split_big_routes`: This parameter is designed to divide larger routes into segments that are more manageable and realistic. It requires two key settings:
    * `max_distance`: The maximum length of a segment before a route is split. This ensures that excessively long routes are broken down into smaller, more accurate segments.
    * `avg_distance`: The average expected length of each segment. This value is used to determine the optimal number of segments for a given route, aiming for segments that closely match this average distance.
    These settings help in reconfiguring routes to better reflect realistic travel patterns, enhancing the accuracy of the data analysis.

  Example:
  ```yaml
  split_big_routes:
    max_distance: 4200 # Maximum distance in meters before splitting a route.
    avg_distance: 2000 # Target average distance in meters for each route segment.

Ensure these parameters are correctly configured to meet the specific needs of the data processing script.

### Optional `processing_date` Parameter for Scripts
Both the extraction and processing scripts accept an optional parameter named `processing_date`. This parameter is used to specify the date for which the data should be processed. If not provided, the scripts default to processing data for the previous day. This default is computed within the Lambda function.

Example usage:
- When running a backfill or if you need to process data for a specific date, you can pass the `processing_date` parameter to the scripts.

## Splitting Large Routes
To enhance the processing of data for routes that significantly exceed average distances, we've implemented a feature to automatically split these large routes into smaller, manageable segments. This adjustment not only ensures the realism and accuracy of the route data but also aligns with practical, achievable distances for drivers. Configuration for this feature is set in the transformations_{dataset_type}.yaml file, with parameters specifying the average and maximum distances that dictate the splitting logic.

## Unique Route IDs via DynamoDB
Pipeline utilizes DynamoDB to manage the generation of unique and sequential route IDs, ensuring consistency and uniqueness across our datasets. A new table, RouteIDCounter, has been introduced to maintain an atomic counter for ID assignment, with another table, RouteMappingsUpdated, capturing the mappings between old and new route IDs. This approach ensures each route processed is assigned a unique identifier, enhancing data integrity and traceability.

## Running a Backfill for Tribu Data Pipeline

Backfilling is occasionally necessary in our data pipeline for several reasons:

- **Extraction Script Backfill**: When issues with the Tribu API data are identified and subsequently resolved, we re-run the extraction scripts to retrieve the corrected data.
- **Processing Script Backfill**: As we refine the processing logic and output format, we frequently re-run the processing scripts to ensure the data meets our updated requirements. Each execution requires a fresh GPS to Celo address mapping.
- **Blockchain Publisher Script Backfill**: We perform backfills for this script when deploying new contracts or if certain routes were missed in previous publications. This ensures all relevant routes are published to the Celo blockchain.
- **Mapping GPS to Celo Address Backfill**: When we do any amend regarless assigning or updating GPS Ids to a contact in Airtable database, we need to run again the GPS to Celo Address Synchronization script.

#### Specifying `processing_date` in Backfill
For running a backfill, the `processing_date` can be specified as follows:
1. Navigate to `tribu_datapipeline`.
2. Execute the backfill commands with the `DATE` parameter set to your desired date.

   ```shell
   make backfill_extracting DATASET_TYPE=guajira DATE=YYYY-MM-DD
   make backfill_processing DATASET_TYPE=guajira DATE=YYYY-MM-DD
   ```

   - This approach allows for partial backfills, enabling the extraction and processing steps to be executed separately as needed.

## Data Storage and Structure

The data pipeline utilizes specific S3 bucket prefixes for storing data at different stages:

### S3 Data Storage
- Raw data: `s3://rodaapp-rappidriverchain/tribu_data/`
- Processed data: `s3://rodaapp-rappidriverchain/rappi_driver_routes/`

### Raw Data in `tribu_data` Bucket Prefix

- The raw data from the Tribu API is stored in the `tribu_data` bucket prefix.
- File Structure: The files are organized by date and source. For example:

   ```shell
   tribu_data/date=YYYY-MM-DD/source=roda/tribu_roda_routes.csv
   tribu_data/date=YYYY-MM-DD/source=guajira/tribu_guajira_routes.csv
   ```

   - This structure holds the raw data for each dataset type (Roda and Guajira) on a specific date.

### Processed Data in `rappi_driver_routes` Bucket
- The processed data is stored in the `rappi_driver_routes` bucket prefix.
- File Structure: Similar to the raw data, the processed files are organized by date and source. For example:

   ```shell
   rappi_driver_routes/date=YYYY-MM-DD/source=tribu_roda/tribu_roda_routes.csv
   rappi_driver_routes/date=YYYY-MM-DD/source=tribu_guajira/tribu_guajira_routes.csv
   ```
   - This structure reflects the processed data for each dataset type and date.

This organization of data in S3 ensures ease of access and clear separation between raw and processed data, which is essential for efficient data management and retrieval.

### Data Handling
- `extracting` pushes data to the `tribu_data` bucket prefix.
- `processing` stores transformed data in the `rappi_driver_routes` bucket prefix.

## Data Storage and Lifecycle Policy

As of now, we do not have a lifecycle policy in place for our S3 files. However, implementing such a policy will be considered in the near future to efficiently manage data storage and costs.

## Data Querying and Analysis Tools

### AWS Glue Catalog - `routes` Table
- **Database**: `rappi_driver_db`
- **Table**: `routes`
- **Data Source**: The `routes` table is populated from data stored at `s3://rodaapp-rappidriverchain/rappi_driver_routes/`, which contains the processed data.
- **Usage**: Accessible through Amazon Athena and Trino Visual Editor for advanced data analytics and visualization.

## Repository Structure and New Projects

The repository structure has been reorganized and expanded to include two additional projects:

- **ScoringModel**: Developed by another team member, this project is now part of this repository to facilitate the sharing of Python utilities and Terraform scripts.
- **Contract Deployment for Routes**: A JavaScript project designed to build, publish, and verify a new contract for routes on the Celo blockchain. This activity is primarily a one-time deployment unless there is a need to restart route publications from scratch.

## Repository Access

Please note that this is a private repository for Roda. Access to this repository is restricted to authorized personnel and collaborators within the organization.
