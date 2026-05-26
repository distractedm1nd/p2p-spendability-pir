locals {
  project_description = "Dedicated infrastructure for spendability-pir AVX-512 hosts."
}

resource "digitalocean_project" "spendability_pir" {
  name        = var.project_name
  description = local.project_description
  purpose     = "Service or API"
  environment = var.project_environment
}

resource "digitalocean_tag" "tags" {
  for_each = toset(var.tags)
  name     = each.value
}

resource "digitalocean_droplet" "pir_host" {
  name       = var.droplet_name
  image      = var.droplet_image
  region     = var.region
  size       = var.droplet_size
  ssh_keys   = var.ssh_key_fingerprints
  tags       = [for tag in digitalocean_tag.tags : tag.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = <<-CLOUD_CONFIG
    #cloud-config
    package_update: true
    packages:
      - ca-certificates
      - curl
      - jq
      - htop
    write_files:
      - path: /usr/local/sbin/check-avx512f
        owner: root:root
        permissions: "0755"
        content: |
          #!/usr/bin/env bash
          set -euo pipefail
          if ! grep -qw avx512f /proc/cpuinfo; then
            echo "ERROR: this host does not expose AVX-512F" >&2
            exit 1
          fi
          echo "AVX-512F detected"
    runcmd:
      - /usr/local/sbin/check-avx512f
  CLOUD_CONFIG
}

resource "digitalocean_firewall" "pir_host" {
  name = "${var.droplet_name}-firewall"
  tags = [for tag in digitalocean_tag.tags : tag.name]

  dynamic "inbound_rule" {
    for_each = var.allowed_ssh_cidrs

    content {
      protocol         = "tcp"
      port_range       = "22"
      source_addresses = [inbound_rule.value]
    }
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

resource "digitalocean_project_resources" "spendability_pir" {
  project   = digitalocean_project.spendability_pir.id
  resources = [digitalocean_droplet.pir_host.urn]
}
