data "aws_route53_zone" "main" {
  name         = var.domain_name
  private_zone = false
}


data "aws_cloudfront_cache_policy" "caching_optimized" {
  name = "Managed-CachingOptimized"
}


resource "aws_s3_bucket" "dashboard" {
  bucket = replace(var.wiki_domain_name, ".", "-")

  tags = var.common_tags
}

resource "aws_s3_bucket_public_access_block" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_cloudfront_origin_access_control" "dashboard" {
  name                              = "${var.wiki_domain_name}-oac"
  description                       = "OAC for ${var.wiki_domain_name}"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_acm_certificate" "dashboard" {
  provider          = aws.us_east_1
  domain_name       = var.wiki_domain_name
  validation_method = "DNS"


  tags = var.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.dashboard.domain_validation_options :
    dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      record = dvo.resource_record_value
    }
  }

  zone_id = data.aws_route53_zone.main.zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 60
  records = [each.value.record]
}

resource "aws_acm_certificate_validation" "dashboard" {
  provider                = aws.us_east_1
  certificate_arn         = aws_acm_certificate.dashboard.arn
  validation_record_fqdns = [for record in aws_route53_record.cert_validation : record.fqdn]
}

resource "aws_cloudfront_distribution" "dashboard" {
  enabled             = true
  is_ipv6_enabled     = true
  comment             = var.wiki_domain_name
  default_root_object = "index.html"

  aliases = [
    var.wiki_domain_name,
    var.domain_name
  ]

  origin {
    domain_name              = aws_s3_bucket.dashboard.bucket_regional_domain_name
    origin_id                = "s3-dashboard-origin"
    origin_access_control_id = aws_cloudfront_origin_access_control.dashboard.id
  }

  default_cache_behavior {
    target_origin_id       = "s3-dashboard-origin"
    viewer_protocol_policy = "redirect-to-https"

    allowed_methods = ["GET", "HEAD"]
    cached_methods  = ["GET", "HEAD"]

    compress        = true
    cache_policy_id = data.aws_cloudfront_cache_policy.caching_optimized.id
  }

  custom_error_response {
    error_code         = 403
    response_code      = 200
    response_page_path = "/index.html"
  }

  custom_error_response {
    error_code         = 404
    response_code      = 200
    response_page_path = "/index.html"
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.dashboard.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  tags = var.common_tags
}

data "aws_iam_policy_document" "dashboard_bucket_policy" {
  statement {
    sid = "AllowCloudFrontServicePrincipalReadOnly"

    actions = [
      "s3:GetObject"
    ]

    resources = [
      "${aws_s3_bucket.dashboard.arn}/*"
    ]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.dashboard.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id
  policy = data.aws_iam_policy_document.dashboard_bucket_policy.json
}

resource "aws_route53_record" "wiki" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = var.wiki_domain_name
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.dashboard.domain_name
    zone_id                = aws_cloudfront_distribution.dashboard.hosted_zone_id
    evaluate_target_health = false
  }
}



data "aws_iam_policy_document" "github_actions_assume_role" {
  statement {
    actions = [
      "sts:AssumeRoleWithWebIdentity"
    ]

    principals {
      type        = "Federated"
      identifiers = [var.github_openid_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repository}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "github_actions_deploy" {
  name               = "github-actions-${replace(var.wiki_domain_name, ".", "-")}"
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume_role.json

  tags = var.common_tags
}

data "aws_iam_policy_document" "github_actions_deploy" {
  statement {
    sid = "AllowS3dashboardDeployment"

    actions = [
      "s3:ListBucket"
    ]

    resources = [
      aws_s3_bucket.dashboard.arn
    ]
  }

  statement {
    sid = "AllowS3ObjectDeployment"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "${aws_s3_bucket.dashboard.arn}/*"
    ]
  }

  statement {
    sid = "AllowCloudFrontInvalidation"

    actions = [
      "cloudfront:CreateInvalidation"
    ]

    resources = [
      aws_cloudfront_distribution.dashboard.arn
    ]
  }
}

resource "aws_iam_role_policy" "github_actions_deploy" {
  name   = "deploy-static-site"
  role   = aws_iam_role.github_actions_deploy.id
  policy = data.aws_iam_policy_document.github_actions_deploy.json
}