variable "domain_name" {
  description = "Root domain name."
  type        = string
  default     = "talelkarimchebbi.com"
}

variable "wiki_domain_name" {
  description = "WWW domain name."
  type        = string
  default     = "wiki.talelkarimchebbi.com"
}

variable "github_repository" {
  description = "GitHub repository allowed to deploy the dashboard. Example: TalelKarim/talelkarim-portfolio"
  type        = string
  default     = "TalelKarim/realtime-media-analytics-platform"
}

variable "common_tags" {
  description = "Common tags applied to taggable resources."
  type        = map(string)
}


variable "github_openid_arn" {
  default = "arn:aws:iam::156358246560:oidc-provider/token.actions.githubusercontent.com"
}