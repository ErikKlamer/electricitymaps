terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Local state for now. Move to an S3 backend once the account is bootstrapped:
  # backend "s3" {
  #   bucket       = "<state-bucket>"
  #   key          = "emaps-etl/terraform.tfstate"
  #   region       = "eu-central-1"
  #   use_lockfile = true
  # }
}
