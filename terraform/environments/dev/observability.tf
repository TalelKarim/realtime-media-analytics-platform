module "grafana_aws_integration" {
  source = "../../modules/observability/grafana_aws_integration"

  name_prefix = "${var.project}-${var.environment}"
  environment = var.environment

  grafana_aws_account_id = var.grafana_aws_account_id
  grafana_external_id    = var.grafana_external_id

  tags = var.tags
}