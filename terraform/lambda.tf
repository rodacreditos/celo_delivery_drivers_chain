resource "aws_lambda_function" "tribu_extraction" {
  function_name = "extract_tribu_data"

  package_type = "Image"
  image_uri    = "062988117074.dkr.ecr.us-east-2.amazonaws.com/rodaapp:tribu_extraction"

  role    = aws_iam_role.lambda_exec_role.arn

  timeout = 600  # Timeout in seconds (current value is 10 minutes)
}

resource "aws_lambda_function" "tribu_processing" {
  function_name = "process_tribu_data"

  package_type = "Image"
  image_uri    = "062988117074.dkr.ecr.us-east-2.amazonaws.com/rodaapp:tribu_processing"

  role    = aws_iam_role.lambda_exec_role.arn

  timeout = 600  # Timeout in seconds (current value is 10 minutes)
}

resource "aws_lambda_function" "gps_to_celo_map_sync" {
  function_name = "sync_gps_to_celo_map"

  package_type = "Image"
  image_uri    = "062988117074.dkr.ecr.us-east-2.amazonaws.com/rodaapp:gps_to_celo_map_sync"

  role    = aws_iam_role.lambda_exec_role.arn

  timeout = 600  # Timeout in seconds (current value is 10 minutes)
}

resource "aws_lambda_function" "publish_to_blockchain" {
  function_name = "publish_to_blockchain"

  package_type = "Image"
  image_uri    = "062988117074.dkr.ecr.us-east-2.amazonaws.com/rodaapp:blockchain_publisher"

  role    = aws_iam_role.lambda_exec_role.arn

  timeout = 900  # Timeout in seconds (current value is 15 minutes)
}

resource "aws_lambda_function" "credit_blockchain_publisher" {
  function_name = "credit_blockchain_publisher"

  package_type = "Image"
  image_uri    = "062988117074.dkr.ecr.us-east-2.amazonaws.com/rodaapp:credit_blockchain_publisher"

  role    = aws_iam_role.lambda_exec_role.arn

  timeout = 900  # Timeout in seconds (current value is 15 minutes, maximum valid value)

  memory_size = 256  # increase memory to 256MB

  image_config {
    command = ["credit_blockchain_publisher.handler"] # Correct key for specifying the handler
  }
}

resource "aws_lambda_function" "payment_blockchain_publisher" {
  function_name = "payment_blockchain_publisher"

  package_type = "Image"
  image_uri    = "062988117074.dkr.ecr.us-east-2.amazonaws.com/rodaapp:credit_blockchain_publisher"

  role    = aws_iam_role.lambda_exec_role.arn

  timeout = 900  # Timeout in seconds (current value is 15 minutes, maximum valid value)

  memory_size = 256  # increase memory to 256MB

  image_config {
    command = ["payment_blockchain_publisher.handler"] # Correct key for specifying the handler
  }
}

resource "aws_lambda_function" "scoring_model" {
  function_name = "scoring_model"

  package_type = "Image"
  image_uri    = "062988117074.dkr.ecr.us-east-2.amazonaws.com/rodaapp:roda_scoring_builder"

  role    = aws_iam_role.lambda_exec_role.arn
  timeout = 900
}



resource "aws_iam_role" "lambda_exec_role" {
  name = "lambda_exec_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
      },
    ],
  })
}

resource "aws_iam_policy" "lambda_s3_access" {
  name        = "LambdaS3AccessPolicy"
  description = "Allow Lambda function to access a specific rodaapp-rappidriverchain S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket", # Added action for listing bucket contents
        ],
        Effect = "Allow",
        Resource = [
          "arn:aws:s3:::rodaapp-rappidriverchain/*",
        ],
      },
      {
        Action = [
          "s3:ListBucket",
        ],
        Effect = "Allow",
        Resource = [
          "arn:aws:s3:::rodaapp-rappidriverchain", # Bucket ARN without the '/*' for ListBucket
        ],
      },
    ],
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_s3_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
