provider "aws" {
  region = "us-east-2"  # Ohio
}

terraform {
  backend "s3" {
    bucket  = "rodaapp-rappidriverchain"
    key     = "terraform/rodaapp.tfstate"
    region  = "us-east-2"
    encrypt = true
  }
}