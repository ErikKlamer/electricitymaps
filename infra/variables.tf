variable "project" {
  description = "Name prefix for all resources."
  type        = string
  default     = "emaps-etl"
}

variable "aws_region" {
  description = "AWS region for the data resources."
  type        = string
  default     = "eu-central-1"
}

variable "aws_profile" {
  description = "AWS CLI profile used by Terraform (null = default credential chain)."
  type        = string
  default     = null
}

variable "bucket_name" {
  description = "Data lake bucket name. Defaults to <project>-<account_id>-<region>."
  type        = string
  default     = null
}

variable "noncurrent_version_retention_days" {
  description = "Days to keep noncurrent object versions before expiry."
  type        = number
  default     = 30
}

variable "api_key_parameter_name" {
  description = "Name of the SSM SecureString parameter holding the Electricity Maps API key (created outside Terraform)."
  type        = string
  default     = "/emaps-etl/electricitymaps/api-key"
}

variable "writer_principal_arns" {
  description = "IAM users/roles allowed to assume the writer role. Empty = the identity running Terraform."
  type        = list(string)
  default     = []
}
