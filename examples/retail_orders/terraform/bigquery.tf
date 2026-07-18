# Dataset/table names below must match ORDERS_DOMAIN's raw_table/dlq_table/
# enriched_table in examples/retail_orders/pipeline.py, and the alerts_table
# passed to cli_main() there ("raw.alerts").
#
# Tables are pre-created with explicit schemas rather than relying on
# WriteToBigQuery's CREATE_IF_NEEDED + autodetect: BigQuery's Storage Write
# API (the framework's default write_method) does not support schema
# autodetection on first write the way legacy streaming inserts sometimes
# do — the table needs to already exist with a matching schema before the
# pipeline runs.

resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  project    = var.project_id
  location   = var.bigquery_dataset_location
  labels     = var.labels
}

resource "google_bigquery_dataset" "enriched" {
  dataset_id = "enriched"
  project    = var.project_id
  location   = var.bigquery_dataset_location
  labels     = var.labels
}

# raw.order_events — one row per validated/enriched order event.
# Field set matches ENVELOPE_REQUIRED + PAYLOAD_REQUIRED in pipeline.py, plus
# ingested_at (stamped by EnrichEvent). Extra payload fields beyond the
# required 4 would be dropped by BigQuery on write (no autodetect at write
# time) — widen the payload RECORD here if your events carry more.
resource "google_bigquery_table" "order_events" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "order_events"
  project    = var.project_id
  labels     = var.labels

  schema = jsonencode([
    { name = "event_id", type = "STRING", mode = "REQUIRED" },
    { name = "event_type", type = "STRING", mode = "REQUIRED" },
    { name = "source_system", type = "STRING", mode = "REQUIRED" },
    { name = "domain", type = "STRING", mode = "REQUIRED" },
    { name = "public_id", type = "STRING", mode = "REQUIRED" },
    { name = "timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    {
      name = "payload", type = "RECORD", mode = "REQUIRED",
      fields = [
        { name = "order_id", type = "STRING", mode = "REQUIRED" },
        { name = "channel", type = "STRING", mode = "REQUIRED" },
        { name = "region", type = "STRING", mode = "REQUIRED" },
        { name = "order_total", type = "FLOAT64", mode = "REQUIRED" },
      ]
    },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# raw.order_events_dlq — malformed/invalid events. Deliberately permissive:
# rows can come from four different failure points (parse, validate, enrich,
# key-by-region), each with a different field set, so almost everything here
# is NULLABLE and `payload` is typed JSON (not RECORD) since DLQ input is by
# definition not known-good — a RECORD schema would itself reject the kind
# of malformed payload this table exists to capture.
resource "google_bigquery_table" "order_events_dlq" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "order_events_dlq"
  project    = var.project_id
  labels     = var.labels

  schema = jsonencode([
    { name = "_error", type = "STRING", mode = "REQUIRED" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "raw", type = "STRING", mode = "NULLABLE" }, # ParseMessage failures
    { name = "event_id", type = "STRING", mode = "NULLABLE" },
    { name = "event_type", type = "STRING", mode = "NULLABLE" },
    { name = "source_system", type = "STRING", mode = "NULLABLE" },
    { name = "domain", type = "STRING", mode = "NULLABLE" },
    { name = "public_id", type = "STRING", mode = "NULLABLE" },
    { name = "timestamp", type = "STRING", mode = "NULLABLE" }, # kept as STRING, not TIMESTAMP — malformed input may not parse
    { name = "payload", type = "JSON", mode = "NULLABLE" },
    { name = "channel", type = "STRING", mode = "NULLABLE" }, # top-level, from an aggregate-stage DLQ row's key_field_names
    { name = "region", type = "STRING", mode = "NULLABLE" },
    { name = "_pipeline_version", type = "STRING", mode = "NULLABLE" },
  ])
}

# enriched.order_summary_5min — one row per (event_type, channel, region)
# per 5-minute window, written by the CombineFn aggregation branch.
resource "google_bigquery_table" "order_summary_5min" {
  dataset_id = google_bigquery_dataset.enriched.dataset_id
  table_id   = "order_summary_5min"
  project    = var.project_id
  labels     = var.labels

  schema = jsonencode([
    { name = "event_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "total_order_value", type = "FLOAT64", mode = "REQUIRED" },
    { name = "cancelled_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "refunded_amount", type = "FLOAT64", mode = "REQUIRED" },
    { name = "computed_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "event_type", type = "STRING", mode = "REQUIRED" },
    { name = "channel", type = "STRING", mode = "REQUIRED" },
    { name = "region", type = "STRING", mode = "REQUIRED" },
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# raw.alerts — shared across every domain passed to cli_main(); this example
# only has one domain ("orders"), so the schema below reflects evaluate_order_alerts'
# _alert() shape specifically. A multi-domain deployment would need a schema
# covering every domain's alert context, or a JSON `context` column instead.
resource "google_bigquery_table" "alerts" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "alerts"
  project    = var.project_id
  labels     = var.labels

  schema = jsonencode([
    { name = "alert_id", type = "STRING", mode = "REQUIRED" },
    { name = "alert_type", type = "STRING", mode = "REQUIRED" },
    { name = "domain", type = "STRING", mode = "REQUIRED" },
    { name = "severity", type = "STRING", mode = "REQUIRED" },
    { name = "window_start", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "window_end", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "metric_name", type = "STRING", mode = "REQUIRED" },
    { name = "metric_value", type = "FLOAT64", mode = "REQUIRED" },
    { name = "threshold", type = "FLOAT64", mode = "REQUIRED" },
    {
      name = "context", type = "RECORD", mode = "NULLABLE",
      fields = [
        { name = "event_type", type = "STRING", mode = "NULLABLE" },
        { name = "channel", type = "STRING", mode = "NULLABLE" },
        { name = "region", type = "STRING", mode = "NULLABLE" },
        { name = "event_count", type = "INTEGER", mode = "NULLABLE" },
      ]
    },
    { name = "triggered_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}
