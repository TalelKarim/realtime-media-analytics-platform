module "sns" {
  source = "../../modules/sns"

  project     = var.project
  environment = var.environment

  email_subscriptions = var.sns_email_subscriptions
}