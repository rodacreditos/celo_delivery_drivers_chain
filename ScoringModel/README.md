# Scoring Model

Brief Description: This project is a scoring model implemented as an AWS Lambda function. It processes data from Airtable, calculates scores based on predefined criteria, and returns the results.

## Features
- Processes data from Airtable.
- Calculates scores using predefined criteria.
- Implemented as an AWS Lambda function within a Docker container.

## Prerequisites

- Docker installed on your local machine.
- AWS CLI configured with appropriate credentials.
- Access to Amazon ECR and AWS Lambda.

## Installation and Setup

### Clone the Repository

```sh
git clone [repository URL]
cd ScoringModel
```

### Build the Docker Image

```sh
docker build -t my_repo_scoring .
```

### Run Locally
To test the Lambda function locally in a Docker container:


```sh
make run
```

In another terminal, invoke the function:

```sh
curl -XPOST "http://localhost:8080/2015-03-31/functions/function/invocations" -d '{}'

```

### Upload the Image to Amazon ECR

```sh
make login-ecr
make push-ecr
```

### Usage in AWS Lambda

Once the image is uploaded to ECR, you can configure an AWS Lambda function to use this image. Ensure you assign appropriate permissions to the Lambda function to interact with other AWS services if necessary.