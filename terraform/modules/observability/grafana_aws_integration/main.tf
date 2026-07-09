data "aws_iam_policy_document" "assume_role" {
  statement {
    sid     = "AllowGrafanaCloudAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"
      identifiers = [
        "arn:aws:iam::${var.grafana_aws_account_id}:root"
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.grafana_external_id]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = "${var.name_prefix}-grafana-cloudwatch-readonly"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json

  description = "Read-only role assumed by Grafana Cloud to scrape CloudWatch metrics."

  tags = merge(var.tags, {
    Name        = "${var.name_prefix}-grafana-cloudwatch-readonly"
    Environment = var.environment
    Component   = "observability"
    ManagedBy   = "terraform"
  })
}

data "aws_iam_policy_document" "permissions" {
  statement {
    sid    = "GrafanaCloudWatchMetricsRead"
    effect = "Allow"

    actions = [
      "tag:GetResources",
      "cloudwatch:GetMetricData",
      "cloudwatch:ListMetrics",
      "apigateway:GET",
      "aps:ListWorkspaces",
      "autoscaling:DescribeAutoScalingGroups",
      "dms:DescribeReplicationInstances",
      "dms:DescribeReplicationTasks",
      "ec2:DescribeTransitGatewayAttachments",
      "ec2:DescribeSpotFleetRequests",
      "shield:ListProtections",
      "storagegateway:ListGateways",
      "storagegateway:ListTagsForResource"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_policy" "this" {
  name        = "${var.name_prefix}-grafana-cloudwatch-readonly"
  description = "Read-only permissions for Grafana Cloud CloudWatch metrics integration."
  policy      = data.aws_iam_policy_document.permissions.json

  tags = merge(var.tags, {
    Name        = "${var.name_prefix}-grafana-cloudwatch-readonly"
    Environment = var.environment
    Component   = "observability"
    ManagedBy   = "terraform"
  })
}

resource "aws_iam_role_policy_attachment" "this" {
  role       = aws_iam_role.this.name
  policy_arn = aws_iam_policy.this.arn
}