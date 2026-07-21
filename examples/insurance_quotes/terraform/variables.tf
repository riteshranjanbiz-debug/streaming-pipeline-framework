variable "project_id" {
  description = "GCP project to deploy the insurance_quotes example into."
  type        = string
}

variable "region" {
  description = "Region for BigQuery dataset location lookups. Must match the region used by examples/retail_orders/terraform if that module already ran in this project."
  type        = string
  default     = "us-central1"
}

variable "bigquery_dataset_location" {
  description = "Location of the pre-existing raw/enriched BigQuery datasets (must match what examples/retail_orders/terraform created, since this module reuses those datasets rather than creating its own)."
  type        = string
  default     = "US"
}

variable "dataflow_service_account_id" {
  description = <<-EOT
    Account ID (the part before @) of the existing Dataflow worker service
    account this module reuses. This example does NOT create its own
    service account or IAM bindings — it assumes examples/retail_orders/terraform
    (or an equivalent) already ran in this project and granted this
    account roles/bigquery.dataEditor on the raw/enriched datasets,
    roles/pubsub.editor, roles/bigquery.jobUser, and roles/dataflow.worker
    at the project level, plus roles/storage.objectAdmin on the temp
    bucket below. Those project/dataset-level grants already cover the new
    topic and tables this module adds, so no new IAM resources are needed.
  EOT
  type        = string
  default     = "streaming-pipeline-dataflow"
}

variable "temp_bucket_name" {
  description = "Name of the pre-existing GCS bucket used for Dataflow --temp-location (see dataflow_service_account_id's docstring — reused, not created here)."
  type        = string
  default     = null
}

variable "labels" {
  description = "Labels applied to every resource this module creates."
  type        = map(string)
  default = {
    app = "streaming-pipeline-framework-insurance-quotes-example"
  }
}
