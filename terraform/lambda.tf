resource "aws_lambda_function" "tribu_extraction" {
  function_name = "extract_tribu_data"

  package_type = "Image"
  image_uri    = "062988117074.dkr.ecr.us-east-2.amazonaws.com/rodaapp:tribu_extraction"

  role    = aws_iam_role.lambda_exec_role.arn

  timeout = 600  # Timeout in seconds (current value is 10 minutes)
}

resource "aws_cloudwatch_event_rule" "daily_guajira_tribu_extraction" {
  name                = "daily-guajira-extraction-lambda-trigger"
  description         = "Trigger tribu guajira extraction Lambda function daily at 1 AM UTC-5"
  schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_cloudwatch_event_rule" "daily_roda_tribu_extraction" {
  name                = "daily-roda-extraction-lambda-trigger"
  description         = "Trigger tribu roda extraction Lambda function daily at 1 AM UTC-5"
  schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda_target_guajira_tribu_extraction" {
  rule      = aws_cloudwatch_event_rule.daily_guajira_tribu_extraction.name
  arn       = aws_lambda_function.tribu_extraction.arn

  input_transformer {
    input_paths = {
      time = "$.time"
    }

    # Assuming the time format is like 2021-03-31T12:00:00Z, this will extract the date part
    input_template = jsonencode({
      dataset_type = "guajira"
      processing_date = "$${time[:10]}"
    })
  }
}

resource "aws_cloudwatch_event_target" "lambda_target_roda_tribu_extraction" {
  rule      = aws_cloudwatch_event_rule.daily_roda_tribu_extraction.name
  arn       = aws_lambda_function.tribu_extraction.arn

  input_transformer {
    input_paths = {
      time = "$.time"
    }

    # Assuming the time format is like 2021-03-31T12:00:00Z, this will extract the date part
    input_template = jsonencode({
      dataset_type = "roda"
      processing_date = "$${time[:10]}"
    })
  }
}

resource "aws_lambda_permission" "allow_cloudwatch_guajira" {
  statement_id  = "AllowExecutionFromCloudWatchGuajira"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.tribu_extraction.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_guajira_tribu_extraction.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_roda" {
  statement_id  = "AllowExecutionFromCloudWatchRoda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.tribu_extraction.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_roda_tribu_extraction.arn
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
