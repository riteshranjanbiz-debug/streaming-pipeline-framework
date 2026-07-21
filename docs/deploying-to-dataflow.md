# Deploying a pipeline to Dataflow — a guide for people who've never touched Dataflow

This walks through everything needed to take one of this repo's example pipelines
(`examples/retail_orders/` or `examples/insurance_quotes/`) from "just code on my laptop" to
"actually running and writing real rows to BigQuery" on Google Cloud. It uses
`insurance_quotes` as the running example, but every step is identical for `retail_orders` —
just swap the path.

No prior Dataflow experience assumed. Where a term is Google-specific, it's explained the
first time it shows up.

---

## 1. The picture, in plain English

```
your laptop                          Google Cloud
------------                         -------------------------------------------------
simulate_traffic.py   --publishes-->  Pub/Sub topic ("quote-events")
                                              |
                                              v
                                       Dataflow job (a program that never stops running,
                                       reading events as they arrive)
                                              |
                                              v
                                       BigQuery tables (raw.quote_events, etc.)
```

Four Google Cloud services are involved, and it helps to know what each one's *job* is
before touching any of them:

- **Pub/Sub** — a message queue. `simulate_traffic.py` (or a real application) publishes
  JSON events to a *topic*; nothing happens to them until something *subscribes* and reads
  them.
- **BigQuery** — the database the pipeline writes its output into. Just tables, same as
  any SQL database, queried with `bq query` or the console.
- **Dataflow** — the thing that actually *runs* the pipeline. It reads this repo's Python
  code, figures out how to distribute the work across one or more worker machines
  (see "worker" below), and keeps running continuously for a streaming pipeline like this
  one — it does not stop after processing what's currently in the queue.
- **Terraform** — not a Google service, a tool that creates/deletes the above (topics,
  tables, service accounts) from a text description instead of clicking through a console.
  Already written for you in `examples/insurance_quotes/terraform/` — see §2.

A few words worth knowing before you see them in error messages:

- **Worker** — a virtual machine Dataflow spins up to actually execute the pipeline code.
  For a small pipeline like this, one worker is enough. When people say "the job is
  running but doing nothing," it's almost always a worker-startup problem — see §7.
- **Region / zone** — a region (`us-central1`) is a geographic area; it's made of several
  zones (`us-central1-a`, `-b`, `-c`...). Workers launch into one specific zone. This
  matters because zones sometimes run out of spare machines — see §7.1.
- **Service account** — a non-human Google identity the worker VM runs as, so it can read
  Pub/Sub and write BigQuery without your personal credentials.

---

## 2. Prerequisites (one-time setup)

Run these from the repo root. Skip anything you've already confirmed works.

```bash
# 1. Python environment with this package + Apache Beam's GCP extras
pip install -e ".[dev,gcp]"

# 2. gcloud CLI, logged in and pointed at the right project
gcloud auth login
gcloud auth application-default login   # separate from step above — this is what Python's
                                         # Google client libraries actually read
gcloud config set project <your-gcp-project-id>

# 3. Terraform (only needed to create/destroy infrastructure, not to run the pipeline)
brew install terraform

# 4. A real Java runtime — see the box below for why
brew install openjdk
```

**Why Java, for a Python pipeline?** One BigQuery write mode this framework uses
(`STORAGE_WRITE_API`, the default — see `CLAUDE.md`) is implemented in Java, not Python.
Even though the *pipeline* is Python and runs on Dataflow (not your laptop), your laptop
still needs a working Java runtime for one step: translating ("expanding") that part of the
pipeline into something Dataflow understands, before the job is even submitted. macOS ships
a fake `java` command that just pops up an "install Java" dialog — that doesn't count.
Confirm you have a real one:

```bash
/opt/homebrew/Cellar/openjdk/*/bin/java -version
# should print something like: openjdk version "26.0.1" ...
```

If that works but plain `java -version` doesn't, put it on your `PATH` for the session you
launch the pipeline from:

```bash
export PATH="$(brew --prefix openjdk)/bin:$PATH"
```

---

## 3. Step 1 — create the infrastructure (Terraform)

This creates the Pub/Sub topic and the BigQuery tables the pipeline reads from / writes to.
You only need to do this once per GCP project (re-running it is safe — Terraform only
changes what's actually different).

```bash
cd examples/insurance_quotes/terraform
terraform init                              # downloads the Google provider, one-time
terraform plan  -var="project_id=<your-project>"   # shows what it's ABOUT to create — read this
terraform apply -var="project_id=<your-project>"   # actually creates it
```

`terraform plan` is not optional to skip — it's a preview, and reading it before `apply` is
the single best habit for not being surprised by what Terraform is about to do to your
project.

What this specific module creates, and — importantly — what it *doesn't*:

- Creates: the `quote-events` Pub/Sub topic, and 5 BigQuery tables
  (`raw.quote_events`, `raw.quote_events_dlq`, `raw.quote_alerts`,
  `enriched.quote_funnel_5min`, `enriched.applicant_360`).
- Does **not** create the `raw`/`enriched` BigQuery datasets themselves, a service account,
  or a GCS bucket — this module assumes `examples/retail_orders/terraform` already ran in
  the same project and created those (see `shared.tf`'s comment for why: two Terraform
  modules trying to own the same dataset/bucket/service-account fight each other). If
  you're deploying into a brand-new project that has never run `retail_orders/terraform`,
  read `examples/retail_orders/terraform/apis.tf`/`iam.tf`/`storage.tf` first — you'll need
  those resources created by *something* before this module's `apply` will succeed.

When you're done experimenting and want to stop paying for storage (small, but not zero):

```bash
terraform destroy -var="project_id=<your-project>"
```

---

## 4. Step 2 — package the pipeline correctly (the part everyone gets wrong once)

This is the step that isn't obvious and isn't optional. Skipping it doesn't produce an
error — it produces a job that looks like it's running forever and silently does nothing.
Full explanation of *why* is in §7.2; here's just what to do.

From the repo root:

```bash
pip wheel . -w dist/ --no-deps
```

This packages this repo's own `streaming_pipeline_framework` code into a single file
(`dist/streaming_pipeline_framework-0.1.0-py3-none-any.whl`) that gets uploaded to every
Dataflow worker. Re-run this any time you change code under `src/streaming_pipeline_framework/`
before redeploying — it's a snapshot, not a live link.

You'll pass this file to the launch command in the next step via `--extra_package`. If you
forget it, `cli.py` will refuse to submit the job and tell you to do this — see §7.2.

---

## 5. Step 3 — launch the pipeline

```bash
python -m examples.insurance_quotes.pipeline \
  --project <your-project> \
  --region us-central1 \
  --runner DataflowRunner \
  --temp-location gs://<your-project>-streaming-pipeline-temp/tmp \
  --service-account-email streaming-pipeline-dataflow@<your-project>.iam.gserviceaccount.com \
  --extra_package dist/streaming_pipeline_framework-0.1.0-py3-none-any.whl
```

Where do `--temp-location` and `--service-account-email` come from? Terraform already
created both (in the shared `retail_orders` module) and printed them as *outputs*. Get them
any time with:

```bash
cd examples/retail_orders/terraform   # the module that owns them
terraform output run_command          # prints a ready-to-paste command with real values filled in
```

**This command does not return.** A streaming pipeline runs forever by design — there's no
"finished" state, since new events could arrive at any time. Run it in the background, or in
a separate terminal tab, and move on to the next step. To stop it later, see §6.

For a quick local-only smoke test that skips all of this (no Dataflow job, no cost, but also
can't use `STORAGE_WRITE_API` — see the note in `build_streaming_pipeline`'s docstring),
use `--runner DirectRunner` and pass `write_method="STREAMING_INSERTS"` from your own
`main.py` instead of calling `cli_main` with the defaults.

---

## 6. Step 4 — is it actually working?

**Don't trust the job status alone.** Dataflow shows a job as `RUNNING` the moment it's
accepted the job — that's *before* any worker has actually started, and (as covered in §7)
a worker can fail to start while the job still says `RUNNING`. The only real confirmation is
data landing where it should.

### Check the job status

```bash
gcloud dataflow jobs list --project=<your-project> --region=us-central1 --status=active
```

### Send it some test traffic

```bash
python -m examples.insurance_quotes.simulate_traffic \
  --project <your-project> --applicants 20 --duration 90
```

### Confirm rows landed

```bash
bq query --project_id=<your-project> --use_legacy_sql=false \
  "SELECT event_type, COUNT(*) FROM raw.quote_events GROUP BY event_type"
```

If this comes back empty a minute or two after sending traffic, something's wrong — go to
§7. If it comes back empty *immediately* after sending traffic, that's normal; give it
15-30 seconds.

### Check for anything landing in the "dead letter queue" (DLQ)

Malformed or invalid events go here instead of silently vanishing — this table should
normally be empty:

```bash
bq query --project_id=<your-project> --use_legacy_sql=false \
  "SELECT * FROM raw.quote_events_dlq LIMIT 10"
```

---

## 7. Troubleshooting — the exact problems you're most likely to hit

These are not hypothetical — every one of these happened during the first real deployment
of this example, in order, and cost real debugging time to work out. Reading this section
before you deploy will save you that time.

### 7.1 — job stuck `RUNNING`, worker never starts, logs say `ZONE_RESOURCE_POOL_EXHAUSTED`

**What it means:** Google is temporarily out of spare machines in the specific zone your
worker tried to start in. This is a capacity issue on Google's end, not a bug in the
pipeline or your setup.

**How to see it:**

```bash
gcloud logging read \
  "resource.type=dataflow_step AND resource.labels.job_id=<job-id> AND severity>=ERROR" \
  --project=<your-project> --limit=10 --freshness=10m
```

**Fix:** Cancel the job and relaunch pinned to a different zone:

```bash
gcloud dataflow jobs cancel <job-id> --project=<your-project> --region=us-central1

# retry, adding --worker_zone us-central1-b (or -c, -f) to the launch command from §5
```

If every zone in a region is exhausted (rare, but it happened during initial testing — all
of `us-central1-a/b/c` were out at once), switch `--region` entirely, e.g. to `us-east1`.
The GCS bucket being in a different region than the job runs in is fine for a job this
small — it just adds a little cross-region latency, not a functional problem.

### 7.2 — job says `RUNNING`, no errors, but zero rows ever land in BigQuery

**What it means:** almost always `ModuleNotFoundError: No module named 'streaming_pipeline_framework'`
on the worker — check the same `gcloud logging read` command as above. The short version:
Dataflow workers start from a clean container that only has `apache_beam` installed. This
repo's own code (`streaming_pipeline_framework`) has to be explicitly uploaded to them; it
is *not* included automatically, even though it works fine when you run things locally.

**Fix:** this is exactly what §4 (`pip wheel . -w dist/ --no-deps` + `--extra_package`) is
for. If you skipped it, `cli.py` now refuses to launch the job at all and tells you to run
that command — so if you're on a version of this repo from after this was fixed, you'll see
a clear error instead of a silently-stuck job. If you somehow still hit the silent version,
you're likely on Dataflow's own retry loop from before the fix — cancel and relaunch with
`--extra_package` set correctly.

### 7.3 — `RuntimeError: Service failed to start up` / `Unable to locate a Java Runtime`

**What it means:** covered in §2 — no real JRE on your laptop for Beam's local expansion
step. This happens *before* anything is submitted to Google, so it costs nothing, it's just
annoying.

**Fix:** `brew install openjdk` and put it on `PATH` (§2's exact commands).

### 7.4 — two examples sharing one GCP project, alerts table has the wrong shape

**What it means:** if you ever add a *third* example/domain to this repo that reuses the
`raw`/`enriched` datasets pattern, give its alerts table its own name (like
`quote_alerts` here, not the generic `alerts` that `retail_orders` already uses). BigQuery's
`STORAGE_WRITE_API` write path needs the table's actual schema to exactly match what the
code is writing — two examples with different alert `context` fields can't share one table
name in the same project.

---

## 8. Step 5 — shutting it down

Streaming Dataflow jobs run (and bill) until you cancel them. There's no auto-stop.

```bash
gcloud dataflow jobs list --project=<your-project> --region=us-central1 --status=active
gcloud dataflow jobs cancel <job-id> --project=<your-project> --region=us-central1
```

Always double check nothing's left running across every region you touched while
debugging zone issues (§7.1) — it's easy to launch a retry in a new region and forget the
old job:

```bash
for region in us-central1 us-east1; do
  gcloud dataflow jobs list --project=<your-project> --region=$region --status=active
done
```

Cancelling the job does **not** delete the Pub/Sub topic or BigQuery tables/data — that's
what `terraform destroy` (§3) is for, and it's a separate, deliberate step.

---

## 9. Cost awareness (rough guide, not a guarantee)

- **Cancelled/failed jobs that never got a working worker** (§7.1, §7.3): effectively free —
  you're billed for worker VM time, and no VM ever successfully started.
- **A running job with one small worker, for a short verification window** (tens of
  minutes): a few cents to low dollars — small Compute Engine VM + a bit of Dataflow service
  fee + trivial Pub/Sub/BigQuery usage at this data volume.
- **The real risk isn't the demo run — it's forgetting to cancel it.** A streaming job left
  running silently accrues cost with nothing new happening. Get in the habit of running the
  §8 "list active jobs" command as your last step every session.
- **Terraform infra sitting idle** (topic + empty tables, no job running): effectively free
  — you're not charged for an idle Pub/Sub topic or empty BigQuery tables, only for actual
  usage (storage of real data, running jobs, query volume).

---

## 10. Cheat sheet

```bash
# One-time setup
pip install -e ".[dev,gcp]"
gcloud auth login && gcloud auth application-default login
brew install terraform openjdk
export PATH="$(brew --prefix openjdk)/bin:$PATH"

# Create infra (once per project)
cd examples/insurance_quotes/terraform
terraform init && terraform apply -var="project_id=<project>"

# Every time you deploy
cd /path/to/repo/root
pip wheel . -w dist/ --no-deps
python -m examples.insurance_quotes.pipeline \
  --project <project> --region us-central1 --runner DataflowRunner \
  --temp-location gs://<project>-streaming-pipeline-temp/tmp \
  --service-account-email streaming-pipeline-dataflow@<project>.iam.gserviceaccount.com \
  --extra_package dist/streaming_pipeline_framework-0.1.0-py3-none-any.whl

# Verify
python -m examples.insurance_quotes.simulate_traffic --project <project> --applicants 20 --duration 90
bq query --project_id=<project> --use_legacy_sql=false "SELECT COUNT(*) FROM raw.quote_events"

# Shut down (always do this)
gcloud dataflow jobs list --project=<project> --region=us-central1 --status=active
gcloud dataflow jobs cancel <job-id> --project=<project> --region=us-central1

# Tear down infra entirely, when done for good
cd examples/insurance_quotes/terraform
terraform destroy -var="project_id=<project>"
```
