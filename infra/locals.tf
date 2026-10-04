locals {
  account_id  = data.aws_caller_identity.current.account_id
  partition   = data.aws_partition.current.partition
  bucket_name = coalesce(var.bucket_name, "${var.project}-${local.account_id}-${var.aws_region}")

  writer_principal_arns = coalescelist(var.writer_principal_arns, [data.aws_caller_identity.current.arn])
}
