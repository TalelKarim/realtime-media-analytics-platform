locals {
  athena_results_location = "s3://${var.datalake_bucket_name}/athena-results/quicksight/"

  gold_datasets = {
    wiki_activity_by_hour = {
      data_set_id = "${var.name_prefix}-gold-wiki-activity-by-hour"
      name        = "Gold - Wiki Activity by Hour"
      table_name  = "wikimedia_gold_wiki_activity_by_hour"

      columns = [
        { name = "metric_date", type = "DATETIME" },
        { name = "metric_hour", type = "DATETIME" },
        { name = "wiki", type = "STRING" },
        { name = "event_count", type = "INTEGER" },
        { name = "bot_event_count", type = "INTEGER" },
        { name = "human_event_count", type = "INTEGER" },
        { name = "distinct_titles", type = "INTEGER" },
        { name = "first_event_at", type = "DATETIME" },
        { name = "last_event_at", type = "DATETIME" },
        { name = "year", type = "INTEGER" },
        { name = "month", type = "INTEGER" },
        { name = "day", type = "INTEGER" },
        { name = "hour", type = "INTEGER" }
      ]
    }

    change_type_by_hour = {
      data_set_id = "${var.name_prefix}-gold-change-type-by-hour"
      name        = "Gold - Change Type by Hour"
      table_name  = "wikimedia_gold_change_type_by_hour"

      columns = [
        { name = "metric_date", type = "DATETIME" },
        { name = "metric_hour", type = "DATETIME" },
        { name = "change_type", type = "STRING" },
        { name = "event_count", type = "INTEGER" },
        { name = "first_event_at", type = "DATETIME" },
        { name = "last_event_at", type = "DATETIME" },
        { name = "year", type = "INTEGER" },
        { name = "month", type = "INTEGER" },
        { name = "day", type = "INTEGER" },
        { name = "hour", type = "INTEGER" }
      ]
    }

    bot_activity_by_hour = {
      data_set_id = "${var.name_prefix}-gold-bot-activity-by-hour"
      name        = "Gold - Bot Activity by Hour"
      table_name  = "wikimedia_gold_bot_activity_by_hour"

      columns = [
        { name = "metric_date", type = "DATETIME" },
        { name = "metric_hour", type = "DATETIME" },
        { name = "user_is_bot", type = "BOOLEAN" },
        { name = "event_count", type = "INTEGER" },
        { name = "first_event_at", type = "DATETIME" },
        { name = "last_event_at", type = "DATETIME" },
        { name = "year", type = "INTEGER" },
        { name = "month", type = "INTEGER" },
        { name = "day", type = "INTEGER" },
        { name = "hour", type = "INTEGER" }
      ]
    }

    namespace_activity_by_hour = {
      data_set_id = "${var.name_prefix}-gold-namespace-activity-by-hour"
      name        = "Gold - Namespace Activity by Hour"
      table_name  = "wikimedia_gold_namespace_activity_by_hour"

      columns = [
        { name = "metric_date", type = "DATETIME" },
        { name = "metric_hour", type = "DATETIME" },
        { name = "namespace", type = "INTEGER" },
        { name = "event_count", type = "INTEGER" },
        { name = "first_event_at", type = "DATETIME" },
        { name = "last_event_at", type = "DATETIME" },
        { name = "year", type = "INTEGER" },
        { name = "month", type = "INTEGER" },
        { name = "day", type = "INTEGER" },
        { name = "hour", type = "INTEGER" }
      ]
    }

    top_pages_by_hour = {
      data_set_id = "${var.name_prefix}-gold-top-pages-by-hour"
      name        = "Gold - Top Pages by Hour"
      table_name  = "wikimedia_gold_top_pages_by_hour"

      columns = [
        { name = "metric_date", type = "DATETIME" },
        { name = "metric_hour", type = "DATETIME" },
        { name = "page_rank", type = "INTEGER" },
        { name = "wiki", type = "STRING" },
        { name = "namespace", type = "INTEGER" },
        { name = "title", type = "STRING" },
        { name = "event_count", type = "INTEGER" },
        { name = "bot_event_count", type = "INTEGER" },
        { name = "human_event_count", type = "INTEGER" },
        { name = "first_event_at", type = "DATETIME" },
        { name = "last_event_at", type = "DATETIME" },
        { name = "year", type = "INTEGER" },
        { name = "month", type = "INTEGER" },
        { name = "day", type = "INTEGER" },
        { name = "hour", type = "INTEGER" }
      ]
    }
  }

  data_source_permissions = [
    "quicksight:DescribeDataSource",
    "quicksight:DescribeDataSourcePermissions",
    "quicksight:PassDataSource",
    "quicksight:UpdateDataSource",
    "quicksight:DeleteDataSource",
    "quicksight:UpdateDataSourcePermissions"
  ]

  data_set_permissions = [
    "quicksight:DescribeDataSet",
    "quicksight:DescribeDataSetPermissions",
    "quicksight:PassDataSet",
    "quicksight:DescribeIngestion",
    "quicksight:ListIngestions",
    "quicksight:UpdateDataSet",
    "quicksight:DeleteDataSet",
    "quicksight:CreateIngestion",
    "quicksight:CancelIngestion",
    "quicksight:UpdateDataSetPermissions"
  ]
}




data "aws_iam_role" "quicksight_service_role" {
  name = var.quicksight_service_role_name
}

data "aws_iam_policy_document" "quicksight_athena_s3_access" {
  statement {
    sid    = "ListDataLakeBucket"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      var.datalake_bucket_arn
    ]
  }

  statement {
    sid    = "ReadGoldData"
    effect = "Allow"

    actions = [
      "s3:GetObject"
    ]

    resources = [
      "${var.datalake_bucket_arn}/gold/*"
    ]
  }

  statement {
    sid    = "ReadWriteAthenaQueryResults"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]

    resources = [
      "${var.datalake_bucket_arn}/athena-results/quicksight/*"
    ]
  }

  statement {
    sid    = "UseDataLakeKmsKey"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey"
    ]

    resources = [
      var.s3_kms_key_arn
    ]
  }
}

resource "aws_iam_role_policy" "quicksight_athena_s3_access" {
  name   = "${var.name_prefix}-quicksight-athena-s3-access"
  role   = data.aws_iam_role.quicksight_service_role.id
  policy = data.aws_iam_policy_document.quicksight_athena_s3_access.json
}


resource "aws_athena_workgroup" "quicksight" {
  name = "${var.name_prefix}-quicksight"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = local.athena_results_location

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = var.s3_kms_key_arn
      }
    }
  }

  tags = var.tags
}

resource "aws_quicksight_data_source" "athena" {
  aws_account_id = var.aws_account_id

  data_source_id = "${var.name_prefix}-athena-gold"
  name           = "${var.name_prefix}-athena-gold"
  type           = "ATHENA"

  parameters {
    athena {
      work_group = aws_athena_workgroup.quicksight.name
    }
  }

  permission {
    principal = var.quicksight_principal_arn
    actions   = local.data_source_permissions
  }

  tags = var.tags
}

resource "aws_quicksight_data_set" "gold" {
  for_each = local.gold_datasets

  aws_account_id = var.aws_account_id

  data_set_id = each.value.data_set_id
  name        = each.value.name
  import_mode = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = replace(each.key, "_", "-")
    relational_table {
      data_source_arn = aws_quicksight_data_source.athena.arn
      catalog         = "AwsDataCatalog"
      schema          = var.glue_database_name
      name            = each.value.table_name

      dynamic "input_columns" {
        for_each = each.value.columns

        content {
          name = input_columns.value.name
          type = input_columns.value.type
        }
      }
    }
  }

  permissions {
    principal = var.quicksight_principal_arn
    actions   = local.data_set_permissions
  }

  tags = var.tags
}