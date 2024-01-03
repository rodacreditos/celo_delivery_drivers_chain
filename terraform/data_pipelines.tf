resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "daily-trigger-at-6-am"
  schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_cloudwatch_event_target" "trigger_state_machine" {
  rule = aws_cloudwatch_event_rule.daily_trigger.name
  arn  = aws_sfn_state_machine.tribu_state_machine.arn

  role_arn = aws_iam_role.cloudwatch_role.arn
}

resource "aws_sfn_state_machine" "tribu_state_machine" {
  name     = "TribuStateMachine"
  role_arn = aws_iam_role.sfn_role.arn

  definition = <<EOF
{
  "Comment": "Tribu State Machine",
  "StartAt": "ParallelProcessing",
  "States": {
    "ParallelProcessing": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "GuajiraExtraction",
          "States": {
            "GuajiraExtraction": {
              "Type": "Task",
              "Resource": "${aws_lambda_function.tribu_extraction.arn}",
              "Parameters": {
                "dataset_type": "guajira"
              },
              "Next": "GuajiraProcessing"
            },
            "GuajiraProcessing": {
              "Type": "Task",
              "Resource": "${aws_lambda_function.tribu_processing.arn}",
              "Parameters": {
                "dataset_type": "guajira"
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "RodaExtraction",
          "States": {
            "RodaExtraction": {
              "Type": "Task",
              "Resource": "${aws_lambda_function.tribu_extraction.arn}",
              "Parameters": {
                "dataset_type": "roda"
              },
              "Next": "RodaProcessing"
            },
            "RodaProcessing": {
              "Type": "Task",
              "Resource": "${aws_lambda_function.tribu_processing.arn}",
              "Parameters": {
                "dataset_type": "roda"
              },
              "End": true
            }
          }
        }
      ],
      "End": true
    }
  }
}
EOF
}

resource "aws_iam_role" "sfn_role" {
  name = "sfn_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "sfn_policy" {
  name = "sfn_policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "lambda:InvokeFunction"
        ],
        Effect = "Allow",
        Resource = [
          aws_lambda_function.tribu_extraction.arn,
          aws_lambda_function.tribu_processing.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role" "cloudwatch_role" {
  name = "cloudwatch_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "cloudwatch_sfn_policy" {
  name        = "cloudwatch-sfn-policy"
  description = "Policy to allow CloudWatch to trigger Step Function"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "states:StartExecution",
        Effect = "Allow",
        Resource = aws_sfn_state_machine.tribu_state_machine.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cloudwatch_sfn_attachment" {
  role       = aws_iam_role.cloudwatch_role.name
  policy_arn = aws_iam_policy.cloudwatch_sfn_policy.arn
}