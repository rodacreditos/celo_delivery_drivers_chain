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
        ],
        Effect = "Allow",
        Resource = [
          "arn:aws:s3:::rodaapp-rappidriverchain/*",
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
