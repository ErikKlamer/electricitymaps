output "bucket_name" {
  description = "Data lake bucket (publicly readable)."
  value       = aws_s3_bucket.data.bucket
}

output "storage_uri" {
  description = "Value for EMAPS_STORAGE_URI."
  value       = "s3://${aws_s3_bucket.data.bucket}"
}

output "public_list_command" {
  description = "Browse the data lake without AWS credentials."
  value       = "aws s3 ls s3://${aws_s3_bucket.data.bucket}/ --recursive --no-sign-request"
}

output "writer_role_arn" {
  description = "Role to assume for writing to the data lake."
  value       = aws_iam_role.writer.arn
}

output "api_key_parameter_name" {
  description = "Name of the SSM SecureString parameter holding the API key (created outside Terraform)."
  value       = var.api_key_parameter_name
}

output "aws_config_profile" {
  description = "Profile to add to ~/.aws/config so the scripts assume the writer role."
  value       = <<-EOT
    [profile emaps-writer]
    role_arn = ${aws_iam_role.writer.arn}
    source_profile = ${coalesce(var.aws_profile, "default")}
    region = ${var.aws_region}
  EOT
}
