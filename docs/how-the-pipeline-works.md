# How the pipeline works — a walkthrough of `framework.py`

`docs/deploying-to-dataflow.md` covers the *operational* side (how to launch, verify, and
tear down a job). This is the other half: what the code in
[`src/streaming_pipeline_framework/framework.py`](../src/streaming_pipeline_framework/framework.py)
actually does, method by method — what each piece is called, when Apache Beam calls it, and
how `build_streaming_pipeline` wires them together into the DAG (directed acyclic graph —
the flow chart Dataflow actually executes).

No prior Beam knowledge assumed. Every Beam-specific concept (`DoFn`, `CombineFn`, tagged
outputs, windows, state/timers) is explained the first time it comes up.

---

## 1. The shape, as a diagram

Every one of this repo's example pipelines (`retail_orders`, `insurance_quotes`) is built
from the exact same DAG, repeated once per `DomainSpec` you pass in. This is the whole
picture; every section below is one box in it.

```
Pub/Sub topic
     │
     ▼
  Read              (ReadFromPubSub)
     │
     ▼
  Parse             ParseMessage           ──bad JSON──────────────► DLQ
     │ ok
     ▼
  (Dedup)           DeduplicatePerKey       [only if dedup_key_fn is set]
     │
     ▼
  Validate          ValidateEvent           ──missing/invalid fields─► DLQ
     │ ok
     ▼
  (Inactivity watch) _InactivityWatcher     [only if inactivity_detector is set]
     │ ok  └──synthetic "timed out" event───┐
     │                                       │ (merged back in)
     ▼◄──────────────────────────────────────┘
  Enrich            EnrichEvent             ──enrichment crash───────► DLQ
     │ ok
     ├─────────────────────────────────────────┐
     ▼                                          ▼ (only if enriched_table is set)
  Strip + WriteRaw                          Window                    FixedWindows
  (raw_table)                                   │
                                                 ▼
                                             KeyEvent                 ──key_fn crash──► DLQ
                                                 │ ok
                                                 ▼
                                             CombinePerKey            _DlqWrappingCombineFn
                                                 │
                                                 ▼
                                             AttachWindowAndKey       ──aggregate crash─► DLQ
                                                 │ ok
                                                 ├─────────► WriteAgg (enriched_table)
                                                 ▼ (only if alert_evaluator is set)
                                             DetectAlerts             ──evaluator crash──► DLQ
                                                 │ ok
                                                 ▼
                                             WriteAlerts (alerts_table)
```

Every arrow labeled "DLQ" (dead-letter queue) is the same idea repeated: *something about
this element was invalid or the code that handles it crashed — write it to a table instead
of either silently dropping it or crashing the whole job.* §3 covers how that's implemented
once and reused everywhere.

---

## 2. Two Beam concepts you need before anything else makes sense

**`DoFn`** ("do function") — a class with a `process()` method that Beam calls **once per
element**, whenever an element arrives. Most of the boxes in the diagram above are a
`DoFn`. Think of it as `map()`/`filter()` from a normal collection, except Beam decides
*when* to call it (an element could arrive any time, from a live stream) and *where* (which
worker machine).

**`CombineFn`** — a different shape, used only for the aggregation branch (the bottom half
of the diagram). Instead of one method called once per element, it's four methods with
different jobs, so Beam can start combining elements *before* they're all gathered onto one
worker (see §6 for exactly when each one fires):

- `create_accumulator()` — make an empty running total
- `add_input(accumulator, element)` — fold one more element into it
- `merge_accumulators(list_of_accumulators)` — combine partial totals from different workers
- `extract_output(accumulator)` — turn the final running total into the output row

If you've used `functools.reduce` or written a SQL `GROUP BY ... SUM(...)`, `add_input` is
the reduce step and `extract_output` is what you'd `SELECT` at the end.

---

## 3. The DLQ pattern, explained once (§4 onward just references this)

Almost every `DoFn` in this file does the same thing on failure:

```python
try:
    ... do the real work ...
except Exception as e:
    yield beam.pvalue.TaggedOutput("dlq", _dlq(self.domain, str(e), event))
    return
yield the_successful_result
```

**`TaggedOutput`** is how a `DoFn` sends different elements down different paths — instead
of one output stream, `.with_outputs("dlq", main="ok")` (used everywhere this pattern
appears) splits it into two: `.ok` (the normal path, keeps flowing through the pipeline) and
`.dlq` (routed to `_dlq()`, then eventually written to the domain's `dlq_table`, if one is
configured). This is *not* Python's `try/except` crashing the whole job — a bad element is
quarantined and everything else keeps flowing.

**`_dlq()`** (`framework.py:395`) is the one function that actually builds a DLQ row —
every failure path in the file calls this instead of building its own dict, which is why
`_error` and `ingested_at` always look the same regardless of *which* stage failed
(`health.check_dlq_thresholds` relies on this — it queries any domain's DLQ table without
needing to know which stage produced which row).

---

## 4. Stage by stage, in DAG order

### Read — `beam.io.ReadFromPubSub`

Not custom code — a Beam-provided transform. Reads raw messages (bytes) off the topic named
in `DomainSpec.topic`. This is also where the *first* real Dataflow-specific behavior shows
up: for a streaming job, this transform runs forever, handing elements downstream as they
arrive — it never reaches "done."

### Parse — `ParseMessage` (`framework.py:412`)

**Called:** once per Pub/Sub message.
**Does:** `json.loads(message.data)`. Pub/Sub messages are raw bytes; this turns them into a
Python dict.
**On failure:** malformed JSON → DLQ, with the raw bytes attached (truncated to 500 chars)
so you can see what actually broke.
**On success:** yields the parsed dict, tagged `ok`.

### (Dedup) — `DeduplicatePerKey` — only if `dedup_key_fn` is set

Not custom code. Pub/Sub is *at-least-once*: the same message can be redelivered. If
`DomainSpec.dedup_key_fn` is set (e.g. `lambda e: e["event_id"]`), every event is keyed by
that function and Beam's own `DeduplicatePerKey` drops anything with a key it's already seen
within `dedup_window_secs` (default 600s = 10 minutes). Runs *before* Validate deliberately —
no point spending validation/enrichment work on a duplicate.

### Validate — `ValidateEvent` (`framework.py:430`)

**Called:** once per event, one instance per `DomainSpec` (so multiple domains sharing a
topic each get their own validator with their own rules).
**Does, in order** (first failure wins — it doesn't check everything and report all
problems, it stops at the first one):
1. Every name in `envelope_required` is present as a top-level key.
2. If `enforce_domain_match` is on (the default) and the event has a `domain` field, it
   matches this `DomainSpec.name`. This exists purely to catch two domains accidentally
   reading each other's events off a shared topic.
3. `event["payload"]` is actually a dict (not a string, not missing, not a list).
4. Every name in `payload_required` is present inside `payload`.
**On failure:** DLQ, with a message telling you exactly which check failed and what was
missing/mismatched.
**On success:** yields the event unchanged, tagged `ok`.

### (Inactivity watch) — `_InactivityWatcher` — only if `inactivity_detector` is set

This is the most unusual piece in the file, because it's *stateful* — every other `DoFn`
here treats each element independently and forgets it immediately. This one remembers
something per key, across elements, potentially for a long time. It's what makes "detect
that a customer went quiet for 20 minutes" possible without a fake "customer went quiet"
event ever existing. Full explanation in §5 — the short version:

- Every real event resets a per-key timer.
- If the timer fires before the next event for that key arrives, a synthetic event is
  generated and merged back into the same stream — same shape, same downstream treatment,
  as a real one.

### Enrich — `EnrichEvent` (`framework.py:485`)

**Called:** once per event (real or synthetic — by this point they're indistinguishable).
**Does:** adds two fields — `ingested_at` (current UTC timestamp) and `_pipeline_version`
(whatever string was passed to `build_streaming_pipeline`). The underscore prefix on
`_pipeline_version` is deliberate — see Strip below.
**On failure:** DLQ (this one is defensive rather than expecting real failures — spreading a
dict with `**event` can only fail if `event` isn't actually a dict).

### Strip + WriteRaw — only if `raw_table` is set

`StripInternalFields` (`framework.py:507`) drops any field starting with `_` (currently just
`_pipeline_version`) — it's useful for internal bookkeeping but shouldn't land in the raw
BigQuery table as a real column. Then `_write_bigquery` (§7) writes the row.

If `raw_table` is *not* set on this `DomainSpec`, this whole branch is skipped — that's the
mechanism a second `DomainSpec` uses to build an alternate aggregate view over an event
stream another `DomainSpec` already writes raw (see `enforce_domain_match`'s note in
`DomainSpec`'s docstring for the paired half of this pattern).

### Window → KeyEvent → CombinePerKey → AttachWindowAndKey — only if `enriched_table` is set

This whole branch is the "windowed aggregation" half of the diagram — §6 covers it in full,
since it's different enough from the linear DoFn chain above to deserve its own section.

### DetectAlerts — `DetectAlerts` (`framework.py:514`) — only if `alert_evaluator` is set

**Called:** once per aggregate row (i.e. once per key per window, after aggregation
finishes — not once per raw event).
**Does:** calls the domain-supplied `alert_evaluator(agg) -> list[alert_dict]` function.
Some evaluators return `[]` most of the time and only produce alerts when a threshold is
crossed — that's completely normal, not every window should page anyone.
**On failure:** DLQ, if the evaluator itself throws.
**On success:** yields each alert dict individually (`yield from alerts` — a window that
trips two alert rules yields two separate rows).

---

## 5. Deep dive: how `_InactivityWatcher` actually works

This backs `InactivityDetector` (the config a `DomainSpec` sets on `.inactivity_detector`).
If your domain doesn't need to infer "went quiet" behavior, you can skip this section
entirely — it never runs.

Two new Beam concepts show up here:

- **State** — normally a `DoFn` remembers nothing between calls. `ReadModifyWriteStateSpec`
  declares a piece of per-key storage Beam manages for you — `state.read()` / `state.write()`
  / `state.clear()`. It survives as long as the key keeps being seen.
- **Timer** — a per-key alarm clock. `timer.set(some_future_time)` schedules a callback;
  `timer.clear()` cancels it. This one uses `TimeDomain.REAL_TIME` (processing/wall-clock
  time, not "event time" — it fires based on the actual clock, not on data arriving).

**`process()`** (`framework.py:559`) — called once per real event on this key:
1. Read the current state (or call `initial_state_fn()` if this is the first event ever
   seen for this key).
2. Call `detector.reducer_fn(current_state, event)` to get the new state — this is
   domain-specific logic, e.g. "track the furthest funnel stage reached."
3. Write the new state back.
4. Call `detector.should_fire_fn(new_state)`. If true (this key still has something
   "pending" — e.g. an unbound quote), reset the timer to fire `timeout_secs` from now. If
   false (the domain's own signal that this key resolved itself, e.g. the policy bound),
   clear the timer instead — so a key that resolved itself never fires a false timeout.
5. Yield the real event through, unchanged, regardless of any of the above — a broken
   `reducer_fn` never blocks the actual event from reaching Enrich (wrapped in a broad
   `try/except`, logged rather than raised).

**`_on_timeout()`** (`framework.py:584`, decorated `@on_timer(_INACTIVITY_TIMER)`) — called
by Beam if the timer set in step 4 above ever actually fires (meaning: no new event for this
key arrived before `timeout_secs` elapsed):
1. Re-check `should_fire_fn` against the current state — belt-and-suspenders in case
   something changed between when the timer was set and when it fired.
2. If still pending, call `detector.timeout_event_fn(key, state)` — domain-specific logic
   that builds a full synthetic event dict, shaped exactly like a real one.
3. Yield it tagged `"timeout"` — back in `build_streaming_pipeline`, this gets `Flatten`-ed
   back together with the normal `ok` stream (see the diagram's "merged back in" arrow), so
   from Enrich onward, a synthetic event and a real one are processed identically.
4. Clear the state — this key starts fresh if it ever becomes active again.

**Why this only works with a real clock:** `Timestamp.now()` reads the actual wall clock.
That means unit tests for this class don't run it through a real Beam pipeline (a bounded
test pipeline finishes instantly, long before any realistic timeout could elapse) — instead
`tests/test_framework.py` calls `.process()` and `._on_timeout()` directly with fake
state/timer objects, the same way every other `DoFn` in this file is tested.

---

## 6. Deep dive: the windowed-aggregation branch

This is the bottom half of the §1 diagram, and it's where the four `CombineFn` methods from
§2 actually get called by Beam, and *when*.

**Window** (`_window_transform`, `framework.py:729`) — groups events into 5-minute buckets
(configurable via `window_secs`) by their timestamp. `FixedWindows(300)` means: every event
gets assigned to exactly one non-overlapping 5-minute bucket, aligned to the epoch (so
buckets are always e.g. `12:00:00–12:05:00`, `12:05:00–12:10:00`, regardless of when the job
started). If `allowed_lateness_secs`/`early_firing_secs` are set on the `DomainSpec`, this
also configures a trigger — when a window is allowed to emit results more than once (early,
speculative firings; or a re-fire when a late event shows up after the window would
otherwise have closed).

**KeyEvent** (`framework.py:623`) — same DLQ-wrapping pattern as everything else. Calls
`DomainSpec.key_fn(event)` to compute a grouping key (e.g. `(product_type,)`), yields
`(key, event)` tuples. A `key_fn` that reaches into a missing field and throws goes to DLQ
instead of crashing the bundle.

**CombinePerKey** — not custom code, a Beam-provided transform that takes the
`_DlqWrappingCombineFn` (wrapping your domain's actual `CombineFn`, e.g.
`AggregateQuoteFunnel`) and runs its four methods **per key, per window**:

- `create_accumulator()` — called once, the first time a given (key, window) pair is seen
  on a worker.
- `add_input(accumulator, event)` — called once per event assigned to that (key, window).
  This is where per-event logic lives (e.g. "if `event_type == 'quote:quoted'`, increment
  `quoted_count`").
- `merge_accumulators([...])` — called when Beam needs to combine partial accumulators from
  different workers or different processing bundles into one. **This is the actual payoff of
  using a `CombineFn` instead of `GroupByKey` + a regular `DoFn`**: partial sums can be
  computed independently on different machines *before* all the data for one key has to land
  in the same place, so a "hot" key (one product type, one region — whatever gets a
  disproportionate share of traffic) doesn't bottleneck a single worker the way collecting
  every raw event onto one machine first would.
- `extract_output(accumulator)` — called once, when the window closes (or fires early/late,
  per the trigger config) — turns the final accumulator into the actual output row.

`_DlqWrappingCombineFn` (`framework.py:643`) wraps all four of the above: a failure in
`add_input`/`merge_accumulators` has nowhere to send a DLQ row (an accumulator has no
per-event identity by the time something's gone wrong — Beam's `CombineFn` contract doesn't
give you one), so those just log and drop the bad input, keeping the accumulator unchanged.
A failure in `extract_output` *can* still be captured, since that's a single result, not a
mid-combine accumulator — it returns a special `{"__agg_error__": True, ...}` dict instead
of raising.

**AttachWindowAndKey** (`framework.py:693`) — runs right after `CombinePerKey`. Two jobs:
1. If it sees the `__agg_error__` sentinel from above, routes it to DLQ.
2. Otherwise, stitches the key and window boundaries back onto the output row. This exists
   because a `CombineFn`'s methods have no access to either — Beam's API constraint is that
   the same four methods have to work whether or not the combine is keyed/windowed at all,
   so the framework does this generically here instead of every domain's `CombineFn`
   re-implementing it. `key_field_names` on the `DomainSpec` controls whether/how the key
   gets zipped onto named fields (e.g. `("product_type",)` turns key `("auto",)` into
   `{"product_type": "auto"}` on the output row).

---

## 7. Writing to BigQuery — `_write_bigquery` / `_coerce_timestamps_for_schema`

Every "Write..." box in the diagram funnels through `_write_bigquery` (`framework.py:793`).
One wrinkle worth knowing about: every timestamp this framework produces is a plain ISO 8601
*string* (`datetime.now(timezone.utc).isoformat()`) — the natural format for JSON and for
BigQuery's legacy streaming-insert API. But the newer, faster `STORAGE_WRITE_API` (the
default write method — see `CLAUDE.md`) needs an actual `Timestamp` *object* for any field
the schema declares as `TIMESTAMP`, not a string. `_coerce_timestamps_for_schema`
(`framework.py:757`) walks a row right before writing and converts exactly those fields
(recursing into nested `RECORD` fields too), so every other piece of code in this file can
keep producing plain strings without needing to know or care which write method is active.

DLQ writes are the one exception — they always use the older `STREAMING_INSERTS` method
regardless of what the rest of the pipeline uses, since DLQ rows come from many different
failure points with inconsistent field shapes, which doesn't fit `STORAGE_WRITE_API`'s
requirement of one single, exact, pre-declared schema.

---

## 8. `build_streaming_pipeline` — how it's all wired together

Everything above is a piece; this function (`framework.py:809`) is what assembles them into
one actual Beam pipeline, once per `DomainSpec` in the list you pass it.

**Before touching Beam at all**, it validates config that would otherwise fail confusingly
deep inside pipeline construction: an `alert_evaluator` without an `alerts_table`, or (when
using `STORAGE_WRITE_API`) a table with no schema. Better to fail immediately with a clear
message than have Beam raise something cryptic mid-DAG-construction.

**Then, for each domain**, in order (this is a literal reading of the `for spec in domains:`
loop): Read → Parse → (Dedup) → Validate → (Inactivity watch, flattened back in) → Enrich →
[Strip → WriteRaw] and, separately, [Window → KeyEvent → CombinePerKey →
AttachWindowAndKey → WriteAgg → (DetectAlerts → WriteAlerts)] — each bracketed group only
built at all if the relevant `DomainSpec` fields are set. Every DLQ-tagged branch from every
stage that ran gets collected into a list and `Flatten`-ed into one stream, written once to
`spec.dlq_table` if one is configured.

One detail that trips people up: this whole function's body is inside
`with beam.Pipeline(options=options) as p:`. Beam's `Pipeline` context manager calls
`.run()` and then `.wait_until_finish()` on exit — for a *streaming* pipeline, that second
call **never returns** (there's no "finished" state for a job reading a live, unbounded
stream). This is exactly why launching the pipeline from `cli.py` (§9) blocks forever and
has to be run in the background or a separate terminal — see
`docs/deploying-to-dataflow.md` §5.

---

## 9. `cli.py` — how a domain's `pipeline.py` actually gets run

`cli.py`'s `main()` is the thing every example's `if __name__ == "__main__":` block calls.
It's deliberately thin:

1. Parses standard flags (`--project`, `--region`, `--runner`, `--temp-location`,
   `--service-account-email`) plus anything else via `parse_known_args` — any extra flag you
   pass (like `--extra_package`, see the deployment doc) flows straight through to Beam's own
   `PipelineOptions` unchanged.
2. Runs `_check_dataflow_worker_packaging` — a fail-fast check specific to `DataflowRunner`
   (see `docs/deploying-to-dataflow.md` §7.2 for exactly what it's guarding against).
3. Builds `PipelineOptions` with `streaming=True` and `save_main_session=True` always set —
   this is a streaming pipeline unconditionally, and `save_main_session` is what lets
   functions/classes defined directly in your `pipeline.py`'s `__main__` module (like a
   domain's `key_fn`, `alert_evaluator`) get shipped to Dataflow workers correctly.
4. Calls `build_streaming_pipeline` (§8) inside `run_with_incident_on_failure`
   (`health.py`) — if the pipeline crashes with an uncaught exception, an optional
   `incident_notifier` (e.g. `ServiceNowClient`) gets a chance to create an incident before
   the exception is re-raised. This is a *separate* mechanism from the DLQ pattern in §3 —
   DLQ handles bad *data*; this handles the pipeline itself dying.

---

## 10. Worked example: tracing one event end-to-end

Take a single real event published to the `insurance_quotes` example's topic:

```json
{"event_type": "quote:quoted", "domain": "quotes",
 "payload": {"quote_id": "QT-1", "product_type": "auto", "premium": 120.5, ...}}
```

1. **Read** picks it up off Pub/Sub as raw bytes.
2. **Parse** (`ParseMessage.process`) — valid JSON, decodes fine → yields the dict, tag `ok`.
3. **Validate** (`ValidateEvent.process`) — has all required envelope/payload fields, domain
   matches → yields unchanged, tag `ok`.
4. **Inactivity watch** (`_InactivityWatcher.process`) — this quote's key (`quote_id`)
   already has state from earlier events; `reducer_fn` sees `quote:quoted` and advances the
   tracked stage to `"quoted"`; `should_fire_fn` says yes (unbound), so the 60-second timer
   resets. The real event is yielded through unchanged, same as always.
5. **Enrich** (`EnrichEvent.process`) — adds `ingested_at` and `_pipeline_version`.
6. Two things happen to this same enriched event, independently:
   - **Strip + WriteRaw**: `_pipeline_version` is dropped, the row lands in `raw.quote_events`.
   - **Window**: assigned to whichever 5-minute bucket its timestamp falls in.
     **KeyEvent**: `key_fn` computes `("auto",)`. **CombinePerKey**: `add_input` is called on
     the `"auto"` accumulator for that window, incrementing `quoted_count`.
     **AttachWindowAndKey**: (later, when the window closes) stitches `product_type: "auto"`
     and the window boundaries onto the final row. **WriteAgg**: lands in
     `enriched.quote_funnel_5min`.

Now take the *other* kind of event — one that's never actually published, but synthesized:
20 minutes (or, in the shortened test config, 60 seconds) after this quote's last real
event, if it's still unbound, `_InactivityWatcher._on_timeout` fires: builds a
`quote:abandoned` event via `timeout_event_fn`, tagged `"timeout"`. Back in
`build_streaming_pipeline`, that stream is `Flatten`-ed together with the normal `ok` stream
— from step 5 (**Enrich**) onward, this synthetic event goes through the *exact same* path
as the real one above. Nothing downstream needs to know or care that it wasn't published by
a real application.

---

## 11. Where to look next

- **`DomainSpec`'s own docstring** (`framework.py:183`) — the full reference for every
  config field, if this doc's summary of one isn't enough.
- **`examples/insurance_quotes/pipeline.py`** or **`examples/retail_orders/pipeline.py`** —
  a complete, concrete `DomainSpec` + `CombineFn` + alert evaluator, to see every concept
  above used for real.
- **`tests/test_framework.py`** — every `DoFn`/`CombineFn` method described here has a
  direct unit test calling it exactly the way this doc describes Beam calling it — reading
  the tests is a good way to see the input/output shape of each method concretely.
- **`docs/deploying-to-dataflow.md`** — how to actually run all of this on GCP.
