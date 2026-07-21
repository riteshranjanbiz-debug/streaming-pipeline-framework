output "service_account_email" {
  description = "Pass this to `python -m examples.insurance_quotes.pipeline --service-account-email`."
  value       = data.google_service_account.dataflow_worker.email
}

output "temp_bucket" {
  description = "Pass this (as gs://<bucket>/tmp) to --temp-location."
  value       = "gs://${data.google_storage_bucket.dataflow_temp.name}/tmp"
}

output "topic" {
  value = google_pubsub_topic.quote_events.name
}

output "run_command" {
  description = "Ready-to-run DataflowRunner invocation using this module's outputs."
  value       = <<-EOT
    python -m examples.insurance_quotes.pipeline \
      --project ${var.project_id} \
      --runner DataflowRunner \
      --region ${var.region} \
      --temp-location gs://${data.google_storage_bucket.dataflow_temp.name}/tmp \
      --service-account-email ${data.google_service_account.dataflow_worker.email}
  EOT
}
