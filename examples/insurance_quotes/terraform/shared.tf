# Resources this module reuses rather than creates. insurance_quotes shares
# the same GCP project as examples/retail_orders in this repo's demo setup
# — the raw/enriched BigQuery datasets, the Dataflow worker service
# account, and the GCS temp bucket are already provisioned by that
# module's Terraform state. Re-declaring them as managed resources here
# would either fail (dataset/bucket/SA already exists) or fight the other
# module's state for ownership, so they're looked up as data sources
# instead — this module only ever creates net-new resources (the
# quote-events topic + the 5 insurance-specific tables).
#
# If you're deploying insurance_quotes into a project that has NOT already
# run examples/retail_orders/terraform, adapt this file to create these
# resources instead of looking them up — see that module's apis.tf,
# iam.tf, and storage.tf for the equivalent resource blocks.

locals {
  temp_bucket_name = coalesce(var.temp_bucket_name, "${var.project_id}-streaming-pipeline-temp")
}

data "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  project    = var.project_id
}

data "google_bigquery_dataset" "enriched" {
  dataset_id = "enriched"
  project    = var.project_id
}

data "google_service_account" "dataflow_worker" {
  account_id = var.dataflow_service_account_id
  project    = var.project_id
}

data "google_storage_bucket" "dataflow_temp" {
  name = local.temp_bucket_name
}
