resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "daily-trigger-at-6-am"
  schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_cloudwatch_event_rule" "daily_trigger_for_credit" {
  name                = "daily-trigger-for-credit-at-6-am"
  schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_cloudwatch_event_target" "trigger_state_machine" {
  rule = aws_cloudwatch_event_rule.daily_trigger.name
  arn  = aws_sfn_state_machine.tribu_state_machine.arn

  role_arn = aws_iam_role.cloudwatch_role.arn
}

resource "aws_cloudwatch_event_target" "trigger_credit_state_machine" {
  rule      = aws_cloudwatch_event_rule.daily_trigger_for_credit.name
  arn       = aws_sfn_state_machine.credit_blockchain_publisher_pipeline.arn
  role_arn  = aws_iam_role.cloudwatch_role.arn
}

resource "aws_sfn_state_machine" "tribu_state_machine" {
  name     = "TribuStateMachine"
  role_arn = aws_iam_role.sfn_role.arn

  definition = <<EOF
{
  "Comment": "Tribu State Machine",
  "StartAt": "GpsToCeloMapping",
  "States": {
    "GpsToCeloMapping": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.gps_to_celo_map_sync.arn}",
      "Next": "ParallelProcessing"
    },
    "ParallelProcessing": {
      "Type": "Parallel",
      "Next": "RodaBlockchainPublisher",
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
      ]
    },
    "RodaBlockchainPublisher": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.publish_to_blockchain.arn}",
      "Parameters": {
        "environment": "staging",
        "timeout": 900
      },
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "IntervalSeconds": 60,
          "MaxAttempts": 25,
          "BackoffRate": 1
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
          aws_lambda_function.gps_to_celo_map_sync.arn,
          aws_lambda_function.tribu_extraction.arn,
          aws_lambda_function.tribu_processing.arn,
          aws_lambda_function.credit_blockchain_publisher.arn,
          aws_lambda_function.payment_blockchain_publisher.arn,
          aws_lambda_function.publish_to_blockchain.arn,
          aws_lambda_function.scoring_model.arn
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
        Resource = [
          aws_sfn_state_machine.tribu_state_machine.arn,
          aws_sfn_state_machine.credit_blockchain_publisher_pipeline.arn
        ]
      },
      {
        Action = "lambda:InvokeFunction",
        Effect = "Allow",
        Resource = [
          aws_lambda_function.scoring_model.arn,
          aws_lambda_function.publish_to_blockchain.arn,
          aws_lambda_function.credit_blockchain_publisher.arn,
          aws_lambda_function.payment_blockchain_publisher.arn
        ]  # Allow scoring lambda function
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cloudwatch_sfn_attachment" {
  role       = aws_iam_role.cloudwatch_role.name
  policy_arn = aws_iam_policy.cloudwatch_sfn_policy.arn
}

resource "aws_sfn_state_machine" "credit_blockchain_publisher_pipeline" {
  name     = "credit_blockchain_publisher_pipeline"
  role_arn = aws_iam_role.sfn_role.arn

  # This pipeline comprises 2 sequential tasks, each configured to timeout after 15 minutes and capable of retrying up to 44 times.
  # Assuming the maximum retry limit is reached, and considering the IntervalSeconds is set to 60 (the pause between retries),
  # the total wait time for retries of a single task is approximately 44 minutes (44 retries * 60 seconds).
  # Since tasks run sequentially and each can run for a maximum duration of 11 hours (if all retries are utilized), 
  # the combined maximum duration for both tasks, excluding the execution time, is approximately 22 hours for retries alone.
  # Including the initial execution time (15 minutes per task before retries) and potential wait time between retries, 
  # the total pipeline execution time could approach up to approximately 22.5 hours, assuming maximum retry durations and wait times.
  definition = <<EOF
{
  "Comment": "Credits and payments publisher to Celo pipeline",
  "StartAt": "CreditBlockchainPublisher",
  "States": {
    "CreditBlockchainPublisher": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.credit_blockchain_publisher.arn}",
      "Parameters": {
        "environment": "staging"
      },
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "IntervalSeconds": 60,
          "MaxAttempts": 44,
          "BackoffRate": 1
        }
      ],
      "Next": "PaymentBlockchainPublisher"
    },
    "PaymentBlockchainPublisher": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.payment_blockchain_publisher.arn}",
      "Parameters": {
        "environment": "staging"
      },
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "IntervalSeconds": 60,
          "MaxAttempts": 44,
          "BackoffRate": 1
        }
      ],
      "End": true
    }
  }
}
EOF
}

resource "aws_sfn_state_machine" "scoring_model_state_machine" {
  name     = "ScoringModelStateMachine"
  role_arn = aws_iam_role.sfn_role.arn

  definition = <<EOF
{
  "Comment": "A state machine to execute the scoring model Lambda function",
  "StartAt": "InvokeScoringModel",
  "States": {
    "InvokeScoringModel": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-2:062988117074:function:scoring_model",
      "End": true
    }
  }
}
EOF
}
