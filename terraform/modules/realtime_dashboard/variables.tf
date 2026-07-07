variable "domain_name" {
  description = "Root domain name."
  type        = string
  default     = "talelkarimchebbi.com"
}

variable "www_domain_name" {
  description = "WWW domain name."
  type        = string
  default     = "realtimeWiki.talelkarimchebbi.com"
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