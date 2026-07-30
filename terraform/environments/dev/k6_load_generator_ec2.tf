# Temporary EC2 load generator for k6 WebSocket tests.
# Location: terraform/environments/dev/k6_load_generator_ec2.tf
#
# Target capacity:
# - Up to ~10,000 persistent WebSocket connections
# - Amazon Linux 2023 x86_64
# - Public subnet in the default VPC
# - Browser-based EC2 Instance Connect over the public IPv4 address
# - k6 and Linux socket tuning installed through user_data

variable "k6_load_generator_enabled" {
  description = "Create the temporary EC2 instance used to run k6 WebSocket load tests."
  type        = bool
  default     = true
}

variable "k6_load_generator_instance_type" {
  description = "EC2 type for the k6 load generator. m7i.4xlarge gives 16 vCPU and 64 GiB RAM."
  type        = string
  default     = "m7i.4xlarge"
}

variable "k6_load_generator_availability_zone" {
  description = "Availability Zone supporting the selected k6 EC2 instance type."
  type        = string
  default     = "us-east-1a"
}

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

data "aws_ssm_parameter" "k6_al2023_ami" {
  count = var.k6_load_generator_enabled ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_ec2_managed_prefix_list" "k6_ec2_instance_connect" {
  count = var.k6_load_generator_enabled ? 1 : 0
  name  = "com.amazonaws.us-east-1.ec2-instance-connect"
}

resource "aws_security_group" "k6_load_generator" {
  count = var.k6_load_generator_enabled ? 1 : 0

  name_prefix = "realtime-media-analytics-dev-k6-"
  description = "EC2 Instance Connect access for the temporary k6 load generator"
  vpc_id      = data.aws_vpc.k6_default[0].id

  ingress {
    description     = "SSH from the regional EC2 Instance Connect service"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    prefix_list_ids = [data.aws_ec2_managed_prefix_list.k6_ec2_instance_connect[0].id]
  }

  # Required for package installation, Git clone and WSS/HTTPS load generation.
  egress {
    description = "Outbound access for package repositories, Git and WebSocket tests"
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
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_instance" "k6_load_generator" {
  count = var.k6_load_generator_enabled ? 1 : 0

  ami                         = data.aws_ssm_parameter.k6_al2023_ami[0].value
  instance_type               = var.k6_load_generator_instance_type
  subnet_id                   = data.aws_subnet.k6_default_public[0].id
  vpc_security_group_ids      = [aws_security_group.k6_load_generator[0].id]
  associate_public_ip_address = true

  # EC2 Instance Connect pushes a short-lived SSH key, so no persistent key pair is required.
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

    # Essential utilities.
    dnf install -y \
      ca-certificates \
      curl \
      git \
      gzip \
      iproute \
      jq \
      procps-ng \
      tar \
      tmux \
      unzip

    # Useful monitoring/debug tools. Do not fail the bootstrap if one optional
    # package is temporarily unavailable in the repository.
    dnf install -y htop iftop bind-utils || true

    # Amazon Linux 2023 normally includes EC2 Instance Connect already.
    rpm -q ec2-instance-connect || dnf install -y ec2-instance-connect || true

    # Official Grafana k6 RPM repository and package.
    dnf install -y https://dl.k6.io/rpm/repo.rpm
    dnf install -y k6

    # Raise the open-file/socket limit for the ec2-user SSH sessions.
    cat >/etc/security/limits.d/99-k6.conf <<'LIMITS'
    ec2-user soft nofile 250000
    ec2-user hard nofile 250000
    LIMITS

    # Also raise the default systemd limit for services/tools started through systemd.
    mkdir -p /etc/systemd/system.conf.d
    cat >/etc/systemd/system.conf.d/99-k6-limits.conf <<'SYSTEMD_LIMITS'
    [Manager]
    DefaultLimitNOFILE=250000
    SYSTEMD_LIMITS

    # Client-side TCP tuning for several thousand persistent outbound sockets.
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
    k6 load generator is ready.

    Verify:
      k6 version
      ulimit -n
      free -h
      nproc

    Clone the project:
      git clone -b v2 <YOUR_GIT_REPOSITORY_URL>
      cd realtime-media-analytics-platform

    Smoke test:
      k6 run \
        -e WS_URL="wss://stream-websocket.talelkarimchebbi.com" \
        -e CONNECTIONS=10 \
        -e HOLD_SECONDS=60 \
        -e RAMP_SECONDS=10 \
        -e SUBSCRIBE_PAYLOAD='{"action":"subscribe","topic":"global"}' \
        load-tests/websocket-connections.js

    10,000-connection test:
      tmux new -s k6-10000

      k6 run \
        -e WS_URL="wss://stream-websocket.talelkarimchebbi.com" \
        -e CONNECTIONS=10000 \
        -e HOLD_SECONDS=900 \
        -e RAMP_SECONDS=300 \
        -e SUBSCRIBE_PAYLOAD='{"action":"subscribe","topic":"global"}' \
        load-tests/websocket-connections.js \
        2>&1 | tee "k6-10000-$(date +%Y%m%d-%H%M%S).log"

    Detach tmux:
      Ctrl+B, then D

    Reattach:
      tmux attach -t k6-10000

    Monitor in other terminals:
      htop
      watch -n 2 'free -h'
      watch -n 2 'ss -Htan state established | wc -l'
      watch -n 2 'ps -C k6 -o pid,%cpu,%mem,rss,vsz,etime,cmd'
    README

    chown ec2-user:ec2-user /home/ec2-user/README-K6.txt

    {
      echo "Bootstrap completed at $(date -Is)"
      k6 version
      sysctl net.ipv4.ip_local_port_range
      sysctl fs.file-max
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