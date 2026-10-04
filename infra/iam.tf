# Writer role for the pipeline scripts. Only the listed IAM principals may assume it.
# Reading needs no role: the bucket is publicly readable.

data "aws_iam_policy_document" "writer_trust" {
  statement {
    sid     = "AllowWriterPrincipals"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "AWS"
      identifiers = local.writer_principal_arns
    }
  }
}

resource "aws_iam_role" "writer" {
  name                 = "${var.project}-writer"
  description          = "Electricity Maps ETL writer role"
  assume_role_policy   = data.aws_iam_policy_document.writer_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "writer" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid = "ReadWriteObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }

  statement {
    sid       = "ReadApiKey"
    actions   = ["ssm:GetParameter"]
    resources = [local.api_key_parameter_arn]
  }
}

resource "aws_iam_role_policy" "writer" {
  name   = "data-lake-write"
  role   = aws_iam_role.writer.id
  policy = data.aws_iam_policy_document.writer.json
}
