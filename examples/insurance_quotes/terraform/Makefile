# Convenience wrapper around Terraform for the insurance_quotes example
# infrastructure. This module only creates the quote-events topic and 5
# tables — it reuses the retail_orders example's datasets/service
# account/bucket via data sources (see shared.tf).
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

.PHONY: init fmt validate plan apply destroy

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

# Deletes only what this module created (topic, subscription, 5 tables) —
# leaves the shared raw/enriched datasets, service account, and bucket
# intact since this module never owned them.
destroy:
	$(TF) destroy $(VAR_ARGS)
