# Tables only — the raw/enriched datasets themselves are looked up in
# shared.tf, not created here (see that file's docstring). Table/field
# names below must match QUOTE_EVENTS_SCHEMA / QUOTE_FUNNEL_SCHEMA /
# APPLICANT_360_SCHEMA / ALERTS_SCHEMA and the raw_table/dlq_table/
# enriched_table/alerts_table strings in examples/insurance_quotes/pipeline.py.
#
# Note: the alerts table is "quote_alerts", not "alerts" — retail_orders
# already owns raw.alerts in this shared project with a different `context`
# RECORD shape (channel/region/event_count vs. this example's
# product_type/bound_count/abandoned_scenario_3_count). STORAGE_WRITE_API
# needs an exact schema match, so the two examples can't share one alerts
# table when deployed into the same project.
#
# deletion_protection = false throughout — same rationale as retail_orders/
# terraform/bigquery.tf: disposable demo data, schema changes here force
# table replacement rather than an in-place ALTER.

# raw.quote_events — one row per validated/enriched quote-journey event.
resource "google_bigquery_table" "quote_events" {
  dataset_id          = data.google_bigquery_dataset.raw.dataset_id
  table_id            = "quote_events"
  project             = var.project_id
  labels              = var.labels
  deletion_protection = false

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
        { name = "quote_id", type = "STRING", mode = "REQUIRED" },
        { name = "session_id", type = "STRING", mode = "REQUIRED" },
        { name = "trace_id", type = "STRING", mode = "REQUIRED" },
        { name = "mdm_id", type = "STRING", mode = "REQUIRED" },
        { name = "product_type", type = "STRING", mode = "REQUIRED" },
        { name = "name", type = "STRING", mode = "NULLABLE" },
        { name = "email", type = "STRING", mode = "NULLABLE" },
        { name = "address_line", type = "STRING", mode = "NULLABLE" },
        { name = "city", type = "STRING", mode = "NULLABLE" },
        { name = "state", type = "STRING", mode = "NULLABLE" },
        { name = "zip", type = "STRING", mode = "NULLABLE" },
        { name = "premium", type = "FLOAT64", mode = "NULLABLE" },
        { name = "coverage_summary", type = "STRING", mode = "NULLABLE" },
        { name = "scenario", type = "STRING", mode = "NULLABLE" },
      ]
    },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# raw.quote_events_dlq — malformed/invalid events, from any of the three
# failure points (parse, validate, enrich). Deliberately permissive: almost
# everything NULLABLE, payload typed JSON not RECORD, same rationale as
# retail_orders/terraform/bigquery.tf's order_events_dlq table.
resource "google_bigquery_table" "quote_events_dlq" {
  dataset_id          = data.google_bigquery_dataset.raw.dataset_id
  table_id            = "quote_events_dlq"
  project             = var.project_id
  labels              = var.labels
  deletion_protection = false

  schema = jsonencode([
    { name = "_error", type = "STRING", mode = "REQUIRED" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "raw", type = "STRING", mode = "NULLABLE" }, # ParseMessage failures
    { name = "event_id", type = "STRING", mode = "NULLABLE" },
    { name = "event_type", type = "STRING", mode = "NULLABLE" },
    { name = "source_system", type = "STRING", mode = "NULLABLE" },
    { name = "domain", type = "STRING", mode = "NULLABLE" },
    { name = "public_id", type = "STRING", mode = "NULLABLE" },
    { name = "timestamp", type = "STRING", mode = "NULLABLE" }, # kept as STRING — malformed input may not parse
    { name = "payload", type = "JSON", mode = "NULLABLE" },
    { name = "product_type", type = "STRING", mode = "NULLABLE" }, # top-level, from an aggregate-stage DLQ row's key_field_names
    { name = "_pipeline_version", type = "STRING", mode = "NULLABLE" },
  ])
}

# enriched.quote_funnel_5min — one row per product_type per 5-minute
# window, written by QUOTES_DOMAIN's AggregateQuoteFunnel CombineFn.
resource "google_bigquery_table" "quote_funnel_5min" {
  dataset_id          = data.google_bigquery_dataset.enriched.dataset_id
  table_id            = "quote_funnel_5min"
  project             = var.project_id
  labels              = var.labels
  deletion_protection = false

  schema = jsonencode([
    { name = "event_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "initiated_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "quoted_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "recalculate_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "bound_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "abandoned_scenario_1_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "abandoned_scenario_2_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "abandoned_scenario_3_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "computed_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "product_type", type = "STRING", mode = "REQUIRED" },
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# enriched.applicant_360 — one row per mdm_id per 5-minute window, written
# by APPLICANT_360_DOMAIN's AggregateApplicant360 CombineFn. "Windowed, not
# lifetime" — same caveat as retail_orders' customer_360.
resource "google_bigquery_table" "applicant_360" {
  dataset_id          = data.google_bigquery_dataset.enriched.dataset_id
  table_id            = "applicant_360"
  project             = var.project_id
  labels              = var.labels
  deletion_protection = false

  schema = jsonencode([
    { name = "mdm_id", type = "STRING", mode = "REQUIRED" },
    { name = "event_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "quote_attempts", type = "INTEGER", mode = "REQUIRED" },
    { name = "quoted_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "bound_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "abandoned_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "last_product_type", type = "STRING", mode = "NULLABLE" },
    { name = "computed_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# raw.quote_alerts — see this file's top docstring for why it's not named
# "alerts". Reflects evaluate_quote_alerts' _alert() shape.
resource "google_bigquery_table" "quote_alerts" {
  dataset_id          = data.google_bigquery_dataset.raw.dataset_id
  table_id            = "quote_alerts"
  project             = var.project_id
  labels              = var.labels
  deletion_protection = false

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
        { name = "product_type", type = "STRING", mode = "NULLABLE" },
        { name = "bound_count", type = "INTEGER", mode = "NULLABLE" },
        { name = "abandoned_scenario_3_count", type = "INTEGER", mode = "NULLABLE" },
      ]
    },
    { name = "triggered_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}
