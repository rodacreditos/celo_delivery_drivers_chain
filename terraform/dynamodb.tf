resource "aws_dynamodb_table" "RouteIDCounter" {
  name         = "RouteIDCounter"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "IDType"

  attribute {
    name = "IDType"
    type = "S"
  }
}
