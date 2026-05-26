# DigitalOcean Terraform

This Terraform stack creates a separate DigitalOcean project for `spendability-pir` and provisions one Premium Intel PIR host.

## Host Profile

- Size: `m-8vcpu-64gb-intel`
- CPU: 8 dedicated Premium Intel vCPUs
- RAM: 64 GB
- Disk: 200 GB NVMe boot disk
- OS: Ubuntu 24.04 x64
- Region: `ams3`

DigitalOcean does not currently expose an exact Premium Intel `8 vCPU / 64 GB RAM / 100 GB disk` slug. The selected size is the closest match for CPU and RAM, exceeds the requested disk size, and is available in `ams3` for this account.

The droplet runs a boot-time `/usr/local/sbin/check-avx512f` script so a non-compliant host is flagged visibly in cloud-init logs if the assigned CPU does not expose AVX-512F.

## Credentials

Do not commit DigitalOcean tokens or local `terraform.tfvars` files. The Valar DigitalOcean credentials live in the Infisical `vote` project:

- Project ID: `40862c6d-a089-4355-b405-0477be0ee3b1`
- Environment: `prod`
- Path: `/`
- Token secret: `DO_TOKEN_NEW_ORG`

Export the Infisical secret into Terraform's expected variable name inside the injected shell:

```bash
infisical run --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 --env=prod --path=/ -- \
  sh -c 'export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG"; terraform plan'
```

The command passes the token through the process environment only; it does not write secrets to disk.

## Usage

```bash
cd infra/digitalocean
cp terraform.tfvars.example terraform.tfvars
# Fill in ssh_key_fingerprints and allowed_ssh_cidrs.

infisical run --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 --env=prod --path=/ -- \
  sh -c 'export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG"; terraform init && terraform plan'
```

Apply only after reviewing the plan:

```bash
infisical run --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 --env=prod --path=/ -- \
  sh -c 'export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG"; terraform apply'
```

The firewall opens SSH only to `allowed_ssh_cidrs`, and opens ports 80 and 443 publicly for the Caddy reverse proxy described in `docs/deploy-setup.md`.
