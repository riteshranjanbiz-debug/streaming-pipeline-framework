# A brand-new project has none of these enabled — enable them here so the
# module is self-contained rather than requiring a manual `gcloud services
# enable` pass first. compute.googleapis.com is needed even though nothing
# below references it directly: Dataflow workers run on GCE VMs under the hood.
locals {
  required_apis = [
    "pubsub.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com", # BigQuery Storage Write API (the framework's default write_method)
    "dataflow.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "storage.googleapis.com",
  ]
}

resource "google_project_service" "required" {
  for_each           = toset(local.required_apis)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
