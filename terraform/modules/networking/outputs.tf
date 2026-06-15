# Consumed by:
#   ecs-collector module → private_subnet_ids, ecs_collector_sg_id
#   kms module           → (no direct dependency)
#   iam module           → (no direct dependency)
#   future modules       → vpc_id, vpc_cidr

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = aws_vpc.main.cidr_block
}

output "public_subnet_ids" {
  description = "Public subnet IDs (NAT Gateway)"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnet IDs (ECS Fargate Collector)"
  value       = aws_subnet.private[*].id
}

output "ecs_collector_sg_id" {
  description = "Security group ID for the ECS Fargate Collector"
  value       = aws_security_group.ecs_collector.id
}

output "vpc_endpoints_sg_id" {
  description = "Security group ID for VPC Interface Endpoints"
  value       = aws_security_group.vpc_endpoints.id
}

output "nat_gateway_public_ip" {
  description = "Public IP of the NAT Gateway"
  value       = aws_eip.nat.public_ip
}
