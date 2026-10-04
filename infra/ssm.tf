# The Electricity Maps API key lives in SSM Parameter Store but is deliberately NOT managed
# by Terraform: the aws_ssm_parameter resource reads the decrypted value back into the state
# file. The parameter is created once, out-of-band:
#
#   aws ssm put-parameter --name /emaps-etl/electricitymaps/api-key \
#     --type SecureString --overwrite --value "<api-key>"
#
# Terraform only needs its ARN (built from the name, without reading the parameter) to grant
# the writer role access.
locals {
  api_key_parameter_arn = "arn:${local.partition}:ssm:${var.aws_region}:${local.account_id}:parameter/${trimprefix(var.api_key_parameter_name, "/")}"
}
