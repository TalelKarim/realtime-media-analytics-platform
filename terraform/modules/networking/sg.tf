
# ============================================================
# SECURITY GROUPS
# ============================================================

# ECS Fargate Collector
# Outbound HTTPS only:
#   - port 443 to internet → Wikimedia SSE stream
#   - port 443 to AWS      → Kinesis, CloudWatch Logs (via endpoints)
# No inbound traffic — collector initiates all connections.

resource "aws_security_group" "ecs_collector" {
  name        = "${local.name_prefix}-ecs-collector-sg"
  description = "ECS Fargate SSE Collector — outbound HTTPS only"
  vpc_id      = aws_vpc.main.id

  egress {
    description = "HTTPS outbound — Wikimedia SSE and AWS service endpoints"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-ecs-collector-sg"
  })
}

# VPC Interface Endpoints
# Accepts HTTPS from resources inside the VPC (ECS Fargate).

resource "aws_security_group" "vpc_endpoints" {
  name        = "${local.name_prefix}-vpce-sg"
  description = "VPC Interface Endpoints — accept HTTPS from within VPC"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTPS from VPC CIDR"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-vpce-sg"
  })
}
