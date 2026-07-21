# Topic name must match QUOTES_DOMAIN.topic / APPLICANT_360_DOMAIN.topic in
# examples/insurance_quotes/pipeline.py — both DomainSpecs read from this
# same topic.
resource "google_pubsub_topic" "quote_events" {
  name    = "quote-events"
  project = var.project_id
  labels  = var.labels
}

# build_streaming_pipeline reads with ReadFromPubSub(topic=...), not
# subscription=... — Dataflow creates and manages its own subscription per
# job run, so this one is optional. Included so you can publish test
# messages and inspect them (gcloud pubsub subscriptions pull) without a
# pipeline running.
resource "google_pubsub_subscription" "quote_events_manual" {
  name    = "quote-events-manual-inspect"
  topic   = google_pubsub_topic.quote_events.id
  project = var.project_id
  labels  = var.labels

  message_retention_duration = "86400s" # 24h
  ack_deadline_seconds       = 60
}
