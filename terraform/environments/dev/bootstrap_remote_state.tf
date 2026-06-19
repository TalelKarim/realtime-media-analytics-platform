data "terraform_remote_state" "bootstrap_ecr" {
  backend = "s3"

  config = {
    bucket = var.bootstrap_tfstate_bucket_name
    key    = "bootstrap/ecr/terraform.tfstate"
    region = var.aws_region
  }
}