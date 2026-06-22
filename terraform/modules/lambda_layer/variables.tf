variable "layer_name" {
  description = "Lambda layer name."
  type        = string
}

variable "description" {
  description = "Lambda layer description."
  type        = string
  default     = null
}

variable "source_dir" {
  description = "Path to the prepared layer directory. It must contain a python/ folder."
  type        = string
}

variable "compatible_runtimes" {
  description = "Compatible Lambda runtimes."
  type        = list(string)
  default     = ["python3.12"]
}

variable "compatible_architectures" {
  description = "Compatible Lambda architectures."
  type        = list(string)
  default     = ["arm64"]
}


variable "package_excludes" {
  description = "Files excluded from layer zip."
  type        = list(string)
  default = [
    "__pycache__/*",
    "*.pyc",
    ".DS_Store"
  ]
}