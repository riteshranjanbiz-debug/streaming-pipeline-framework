variable "project_id" {
  description = "GCP project to deploy the retail_orders example into."
  type        = string
}

variable "region" {
  description = "Region for the Dataflow job, GCS temp bucket, and BigQuery dataset location."
  type        = string
  default     = "us-central1"
}

variable "service_account_id" {
  description = "Account ID (the part before @) for the Dataflow worker service account."
  type        = string
  default     = "streaming-pipeline-dataflow"
}

variable "temp_bucket_name" {
  description = <<-EOT
    GCS bucket name for Dataflow --temp-location. Bucket names are globally
    unique across all of GCS, so the default embeds the project ID; override
    if it collides with an existing bucket.
  EOT
  type        = string
  default     = null
}

variable "bigquery_dataset_location" {
  description = "BigQuery dataset location (multi-region, e.g. US/EU, or a specific region)."
  type        = string
  default     = "US"
}

variable "labels" {
  description = "Labels applied to every resource this module creates."
  type        = map(string)
  default = {
    app = "streaming-pipeline-framework-retail-orders-example"
  }
}
