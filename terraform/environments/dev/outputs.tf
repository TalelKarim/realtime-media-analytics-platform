# Phase 1 outputs — networking
# Run `terraform output` after apply to validate

output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

output "vpc_cidr" {
  description = "VPC CIDR"
  value       = module.networking.vpc_cidr
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = module.networking.public_subnet_ids
}

output "private_subnet_ids" {
  description = "Private subnet IDs — ECS Fargate Collector"
  value       = module.networking.private_subnet_ids
}

output "ecs_collector_sg_id" {
  description = "ECS Collector security group ID"
  value       = module.networking.ecs_collector_sg_id
}

output "nat_gateway_public_ip" {
  description = "NAT Gateway public IP"
  value       = module.networking.nat_gateway_public_ip
}
