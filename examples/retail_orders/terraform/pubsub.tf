# Topic name must match ORDERS_DOMAIN.topic in examples/retail_orders/pipeline.py.
resource "google_pubsub_topic" "order_events" {
  name    = "order-events"
  project = var.project_id
  labels  = var.labels
}

# build_streaming_pipeline reads with ReadFromPubSub(topic=...), not
# subscription=... — Dataflow creates and manages its own subscription per
# job run, so this one is optional. It's included anyway so you can publish
# test messages and inspect them (gcloud pubsub subscriptions pull) without
# a pipeline running, and so local/DirectRunner testing has something to bind to.
resource "google_pubsub_subscription" "order_events_manual" {
  name    = "order-events-manual-inspect"
  topic   = google_pubsub_topic.order_events.id
  project = var.project_id
  labels  = var.labels

  message_retention_duration = "86400s" # 24h
  ack_deadline_seconds       = 60
}
