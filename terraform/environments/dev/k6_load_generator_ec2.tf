# -----------------------------------------------------------------------------
# Temporary EC2 load generator for k6 WebSocket tests
# File: terraform/environments/dev/k6_load_generator_ec2.tf
#
# Purpose:
# - Run k6 from AWS instead of the corporate Mac/network
# - Support tests up to roughly 10,000 persistent WebSocket connections
# - Use Amazon Linux 2023 x86_64
# - Deploy in the default VPC, in the default public subnet of us-east-1a
# - Connect through EC2 Instance Connect using the public IPv4 address
# - Install k6 and tune Linux automatically with user_data
# -----------------------------------------------------------------------------

variable "k6_load_generator_enabled" {
  description = "Create the temporary EC2 instance used to run k6 WebSocket load tests."
  type        = bool
  default     = false
}

variable "k6_load_generator_instance_type" {
  description = "EC2 instance type for the k6 generator. m7i.4xlarge provides 16 vCPU and 64 GiB RAM."
  type        = string
  default     = "m7i.4xlarge"
}

variable "k6_load_generator_availability_zone" {
  description = "Availability Zone supporting the selected instance type."
  type        = string
  default     = "us-east-1a"
}

# -----------------------------------------------------------------------------
# Default VPC and the default subnet in the explicitly selected AZ
# -----------------------------------------------------------------------------

data "aws_vpc" "k6_default" {
  count   = var.k6_load_generator_enabled ? 1 : 0
  default = true
}

data "aws_subnet" "k6_default_public" {
  count = var.k6_load_generator_enabled ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.k6_default[0].id]
  }

  filter {
    name   = "availability-zone"
    values = [var.k6_load_generator_availability_zone]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# Always resolve the current Amazon Linux 2023 x86_64 AMI in this region.
data "aws_ssm_parameter" "k6_al2023_ami" {
  count = var.k6_load_generator_enabled ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# AWS-managed source ranges used by browser-based EC2 Instance Connect.
data "aws_ec2_managed_prefix_list" "k6_ec2_instance_connect" {
  count = var.k6_load_generator_enabled ? 1 : 0
  name  = "com.amazonaws.us-east-1.ec2-instance-connect"
}

# -----------------------------------------------------------------------------
# Security group
# -----------------------------------------------------------------------------

resource "aws_security_group" "k6_load_generator" {
  count = var.k6_load_generator_enabled ? 1 : 0

  name_prefix = "realtime-media-analytics-dev-k6-"
  description = "Temporary k6 generator: SSH through EC2 Instance Connect"
  vpc_id      = data.aws_vpc.k6_default[0].id

  ingress {
    description     = "SSH from AWS EC2 Instance Connect in us-east-1"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    prefix_list_ids = [data.aws_ec2_managed_prefix_list.k6_ec2_instance_connect[0].id]
  }

  egress {
    description = "Outbound Internet access for packages, Git and WSS tests"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "realtime-media-analytics-dev-k6-sg"
    Project     = "realtime-media-analytics"
    Environment = "dev"
    Purpose     = "k6-load-generator"
    ManagedBy   = "Terraform"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# -----------------------------------------------------------------------------
# EC2 instance
# -----------------------------------------------------------------------------

resource "aws_instance" "k6_load_generator" {
  count = var.k6_load_generator_enabled ? 1 : 0

  ami                         = data.aws_ssm_parameter.k6_al2023_ami[0].value
  instance_type               = var.k6_load_generator_instance_type
  availability_zone           = var.k6_load_generator_availability_zone
  subnet_id                   = data.aws_subnet.k6_default_public[0].id
  vpc_security_group_ids      = [aws_security_group.k6_load_generator[0].id]
  associate_public_ip_address = true

  # EC2 Instance Connect injects a short-lived SSH public key.
  # No persistent EC2 key pair is required.
  key_name = null

  user_data_replace_on_change = true

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 50
    encrypted             = true
    delete_on_termination = true
  }

  user_data = <<-USER_DATA
    #!/bin/bash
    set -euxo pipefail

    exec > >(tee /var/log/k6-bootstrap.log | logger -t k6-bootstrap -s 2>/dev/console) 2>&1

    dnf update -y

    dnf install -y \
      bind-utils \
      ca-certificates \
      curl \
      git \
      gzip \
      htop \
      iftop \
      iproute \
      jq \
      procps-ng \
      tar \
      tmux \
      unzip

    # Amazon Linux 2023 generally contains EC2 Instance Connect already.
    rpm -q ec2-instance-connect || dnf install -y ec2-instance-connect

    # Official Grafana k6 RPM repository.
    dnf install -y git
    dnf install -y https://dl.k6.io/rpm/repo.rpm
    dnf install -y k6

    # Raise the open-file limit used by ec2-user SSH sessions.
    cat >/etc/security/limits.d/99-k6.conf <<'LIMITS'
    ec2-user soft nofile 250000
    ec2-user hard nofile 250000
    LIMITS

    # Also raise the systemd default for processes started through systemd.
    mkdir -p /etc/systemd/system.conf.d

    cat >/etc/systemd/system.conf.d/99-k6-limits.conf <<'SYSTEMD_LIMITS'
    [Manager]
    DefaultLimitNOFILE=250000
    SYSTEMD_LIMITS

    # Linux client-side tuning for several thousand persistent outbound sockets.
    cat >/etc/sysctl.d/99-k6.conf <<'SYSCTL'
    fs.file-max = 500000
    net.core.somaxconn = 65535
    net.ipv4.ip_local_port_range = 1024 65535
    net.ipv4.tcp_fin_timeout = 15
    net.ipv4.tcp_tw_reuse = 1
    SYSCTL

    sysctl --system

    mkdir -p /home/ec2-user/load-tests
    chown -R ec2-user:ec2-user /home/ec2-user/load-tests

    cat >/home/ec2-user/README-K6.txt <<'README'
    K6 LOAD GENERATOR
    =================

    1. Verify bootstrap

       sudo cloud-init status --wait
       cat /var/log/k6-ready.log
       k6 version
       ulimit -n
       free -h
       nproc

    2. Clone the repository

       git clone -b v2 <YOUR_GIT_REPOSITORY_URL>
       cd realtime-media-analytics-platform

    3. Smoke test

       k6 run \
         -e WS_URL="wss://stream-websocket.talelkarimchebbi.com" \
         -e CONNECTIONS=10 \
         -e HOLD_SECONDS=60 \
         -e RAMP_SECONDS=10 \
         -e SUBSCRIBE_PAYLOAD='{"action":"subscribe","topic":"global"}' \
         load-tests/websocket-connections.js

    4. Test with 5,000 connections

       tmux new -s k6-5000

       k6 run \
         -e WS_URL="wss://stream-websocket.talelkarimchebbi.com" \
         -e CONNECTIONS=5000 \
         -e HOLD_SECONDS=900 \
         -e RAMP_SECONDS=180 \
         -e SUBSCRIBE_PAYLOAD='{"action":"subscribe","topic":"global"}' \
         load-tests/websocket-connections.js \
         2>&1 | tee "k6-5000-$(date +%Y%m%d-%H%M%S).log"

    5. Test with 10,000 connections

       tmux new -s k6-10000

       k6 run \
         -e WS_URL="wss://stream-websocket.talelkarimchebbi.com" \
         -e CONNECTIONS=10000 \
         -e HOLD_SECONDS=900 \
         -e RAMP_SECONDS=300 \
         -e SUBSCRIBE_PAYLOAD='{"action":"subscribe","topic":"global"}' \
         load-tests/websocket-connections.js \
         2>&1 | tee "k6-10000-$(date +%Y%m%d-%H%M%S).log"

    6. Detach and reattach tmux

       Detach:   Ctrl+B, then D
       Reattach: tmux attach -t k6-10000

    7. Monitor the generator from other terminals

       htop
       watch -n 2 'free -h'
       watch -n 2 'ss -Htan state established | wc -l'
       watch -n 2 'ps -C k6 -o pid,%cpu,%mem,rss,vsz,etime,cmd'
    README

    chown ec2-user:ec2-user /home/ec2-user/README-K6.txt

    {
      echo "Bootstrap completed at $(date -Is)"
      echo
      k6 version
      echo
      echo "CPU:"
      nproc
      echo
      echo "Memory:"
      free -h
      echo
      echo "Kernel limits:"
      sysctl fs.file-max
      sysctl net.ipv4.ip_local_port_range
    } >/var/log/k6-ready.log
  USER_DATA

  tags = {
    Name        = "realtime-media-analytics-dev-k6-load-generator"
    Project     = "realtime-media-analytics"
    Environment = "dev"
    Purpose     = "k6-websocket-load-test"
    ManagedBy   = "Terraform"
  }
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "k6_load_generator_instance_id" {
  description = "Instance ID to select in the EC2 Instance Connect console."
  value       = try(aws_instance.k6_load_generator[0].id, null)
}

output "k6_load_generator_public_ip" {
  description = "Public IPv4 address of the temporary k6 load generator."
  value       = try(aws_instance.k6_load_generator[0].public_ip, null)
}

output "k6_load_generator_public_dns" {
  description = "Public DNS name of the temporary k6 load generator."
  value       = try(aws_instance.k6_load_generator[0].public_dns, null)
}
