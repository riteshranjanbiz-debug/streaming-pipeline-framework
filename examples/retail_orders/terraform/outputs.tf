output "service_account_email" {
  description = "Pass this to `python -m examples.retail_orders.pipeline --service-account-email`."
  value       = google_service_account.dataflow_worker.email
}

output "temp_bucket" {
  description = "Pass this (as gs://<bucket>/tmp) to --temp-location."
  value       = "gs://${google_storage_bucket.dataflow_temp.name}/tmp"
}

output "topic" {
  value = google_pubsub_topic.order_events.name
}

output "run_command" {
  description = "Ready-to-run DataflowRunner invocation using this module's outputs."
  value       = <<-EOT
    python -m examples.retail_orders.pipeline \
      --project ${var.project_id} \
      --runner DataflowRunner \
      --region ${var.region} \
      --temp-location gs://${google_storage_bucket.dataflow_temp.name}/tmp \
      --service-account-email ${google_service_account.dataflow_worker.email}
  EOT
}
