output "project_id" {
  description = "DigitalOcean project ID."
  value       = digitalocean_project.spendability_pir.id
}

output "droplet_id" {
  description = "DigitalOcean droplet ID."
  value       = digitalocean_droplet.pir_host.id
}

output "droplet_name" {
  description = "DigitalOcean droplet name."
  value       = digitalocean_droplet.pir_host.name
}

output "droplet_ipv4" {
  description = "Public IPv4 address of the PIR host."
  value       = digitalocean_droplet.pir_host.ipv4_address
}

output "droplet_ipv6" {
  description = "Public IPv6 address of the PIR host."
  value       = digitalocean_droplet.pir_host.ipv6_address
}

output "droplet_urn" {
  description = "DigitalOcean resource URN assigned to the project."
  value       = digitalocean_droplet.pir_host.urn
}
