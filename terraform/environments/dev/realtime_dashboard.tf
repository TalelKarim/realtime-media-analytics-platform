module "realtime_dashboard" {
  source = "../../modules/realtime_dashboard"

  domain_name       = var.domain_name
  
  github_repository = var.github_repository
  common_tags       = var.tags


  providers = {
    aws           = aws
    aws.us_east_1 = aws.us_east_1
  }
}