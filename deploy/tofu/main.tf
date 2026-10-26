# Describes the manifests in ../kubernetes.
# tofu validate does not contact a cluster and does not need cloud credentials.
# There is no backend and no cloud provider. tofu apply is local and optional.

provider "kubernetes" {}

locals {
  manifest_dir   = "${path.module}/../kubernetes"
  manifest_files = fileset(local.manifest_dir, "*.yaml")
}

resource "kubernetes_manifest" "fleetpilot" {
  for_each = local.manifest_files
  manifest = yamldecode(file("${local.manifest_dir}/${each.value}"))
}
