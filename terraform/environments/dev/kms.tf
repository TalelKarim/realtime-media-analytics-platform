module "kms" {
  source = "../../modules/kms"

  project     = var.project
  environment = var.environment

  deletion_window_in_days = 7 # dev — use 30 in production
}