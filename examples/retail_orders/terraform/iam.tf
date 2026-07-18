resource "google_service_account" "dataflow_worker" {
  account_id   = var.service_account_id
  project      = var.project_id
  display_name = "Dataflow worker — streaming-pipeline-framework retail_orders example"
}

# Broader than strictly necessary: ReadFromPubSub(topic=...) (not
# subscription=...) means Dataflow creates and deletes its own subscription
# per job run, which needs project-level pubsub.subscriptions.create —
# roles/pubsub.subscriber alone isn't enough. Tighten to a custom role if
# you'd rather pre-create a fixed subscription and grant only
# roles/pubsub.subscriber on it.
resource "google_project_iam_member" "dataflow_worker_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_bigquery_dataset_iam_member" "dataflow_worker_raw" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_bigquery_dataset_iam_member" "dataflow_worker_enriched" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.enriched.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Running query/load jobs (e.g. the Storage Write API's use of BQ jobs)
# requires project-level jobUser — there's no dataset-scoped equivalent.
resource "google_project_iam_member" "dataflow_worker_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_project_iam_member" "dataflow_worker_dataflow" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_storage_bucket_iam_member" "dataflow_worker_temp_bucket" {
  bucket = google_storage_bucket.dataflow_temp.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Whoever/whatever submits the Dataflow job (a developer's gcloud identity,
# a CI service account) needs roles/iam.serviceAccountUser on this SA to run
# jobs as it — Terraform doesn't know who that is, so it's left as an input.
# Also needs roles/dataflow.developer at the project level (not granted here
# — that's a human/CI-operator permission, not part of this example's
# runtime footprint).
variable "job_submitters" {
  description = "Members (e.g. \"user:you@example.com\", \"serviceAccount:ci@...\") allowed to submit Dataflow jobs as the worker service account."
  type        = list(string)
  default     = []
}

resource "google_service_account_iam_member" "job_submitters" {
  for_each           = toset(var.job_submitters)
  service_account_id = google_service_account.dataflow_worker.name
  role               = "roles/iam.serviceAccountUser"
  member             = each.value
}
