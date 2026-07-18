locals {
  temp_bucket_name = coalesce(var.temp_bucket_name, "${var.project_id}-streaming-pipeline-temp")
}

# --temp-location for the Dataflow job (staging binaries, shuffle spill, etc).
resource "google_storage_bucket" "dataflow_temp" {
  name                        = local.temp_bucket_name
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
  labels                      = var.labels

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required]
}
