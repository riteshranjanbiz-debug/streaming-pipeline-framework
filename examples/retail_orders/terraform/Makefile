# Convenience wrapper around Terraform for the retail_orders example
# infrastructure.
#
# Usage:
#   make plan    PROJECT_ID=my-project
#   make apply   PROJECT_ID=my-project
#   make destroy PROJECT_ID=my-project
#
# Or copy terraform.tfvars.example to terraform.tfvars and set project_id
# there, then omit PROJECT_ID from every invocation.

TF := terraform
VAR_ARGS := $(if $(PROJECT_ID),-var="project_id=$(PROJECT_ID)",)

.PHONY: init fmt validate plan apply destroy destroy-project

init:
	$(TF) init

fmt:
	$(TF) fmt

validate: fmt
	$(TF) validate

plan: validate
	$(TF) plan $(VAR_ARGS)

apply: validate
	$(TF) apply $(VAR_ARGS)

# Deletes every resource this module created (topic, subscription, BigQuery
# datasets/tables, service account + IAM bindings, GCS bucket) but leaves
# the GCP project itself intact — safe to re-`make apply` afterwards.
destroy:
	$(TF) destroy $(VAR_ARGS)

# More drastic: deletes the entire GCP project (not just this module's
# resources), including anything else you may have created in it since.
# Only use this if the project exists solely for this example. GCP holds
# deleted projects in a 30-day recoverable "pending deletion" state, but
# billing for active resources stops immediately.
destroy-project:
	@if [ -z "$(PROJECT_ID)" ]; then echo "PROJECT_ID is required, e.g. make destroy-project PROJECT_ID=my-project" >&2; exit 1; fi
	gcloud projects delete $(PROJECT_ID)
