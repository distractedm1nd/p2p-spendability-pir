variable "digitalocean_token" {
  description = "DigitalOcean API token. Prefer TF_VAR_digitalocean_token from Infisical vote/prod secret DO_TOKEN_NEW_ORG; do not store it in tfvars."
  type        = string
  sensitive   = true
}

variable "project_name" {
  description = "Name for the dedicated DigitalOcean project."
  type        = string
  default     = "spendability-pir"
}

variable "project_environment" {
  description = "DigitalOcean project environment label."
  type        = string
  default     = "Production"

  validation {
    condition     = contains(["Development", "Staging", "Production"], var.project_environment)
    error_message = "project_environment must be one of Development, Staging, or Production."
  }
}

variable "region" {
  description = "DigitalOcean region for the PIR host."
  type        = string
  default     = "ams3"
}

variable "droplet_name" {
  description = "Name of the PIR server droplet."
  type        = string
  default     = "spendability-pir-01"
}

variable "droplet_size" {
  description = "DigitalOcean size slug. The default is the closest Premium Intel 8 vCPU / 64 GB RAM option and includes 200 GB NVMe disk."
  type        = string
  default     = "m-8vcpu-64gb-intel"
}

variable "droplet_image" {
  description = "DigitalOcean image slug."
  type        = string
  default     = "ubuntu-24-04-x64"
}

variable "ssh_key_fingerprints" {
  description = "Existing DigitalOcean SSH key IDs or fingerprints to install on the droplet."
  type        = list(string)
}

variable "allowed_ssh_cidrs" {
  description = "CIDR ranges allowed to SSH to the host."
  type        = list(string)
  default     = []
}

variable "enable_backups" {
  description = "Enable DigitalOcean droplet backups."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags applied to the droplet and firewall."
  type        = list(string)
  default     = ["spendability-pir", "pir-server"]
}
