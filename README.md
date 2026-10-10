# PythonDataAnalysis

## How to run

```powershell
# one-time setup
py -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt

# Part 1: clean events.jsonl, write output/cleaned_events.jsonl,
# output/rejected_events.jsonl, output/metrics.csv
.venv\Scripts\python.exe -m pipeline.run_pipeline events.jsonl --output-dir output

# Part 3: serve output/metrics.csv over HTTP (run Part 1 first - the API
# reads that file at startup)
.venv\Scripts\python.exe -m uvicorn api.main:app --reload

# tests (covers Part 1 and Part 3)
.venv\Scripts\python.exe -m pytest -q
```

## Part 2 - Storage & modeling

**A note on how I'm reasoning about this:** I haven't worked hands-on with BigQuery or Bigtable. What follows is reasoned by analogy to tools I do know - Hive/HDFS for BigQuery, Cassandra for Bigtable - and checked against a quick search of GCP's documentation rather than assumed, since the whole point of this section is to get the specifics right, not just the shape of the idea.

**Raw events -> BigQuery.** This is the same role Hive played for me: a SQL-queryable layer over large volumes of semi-structured data, for consumers (analytics, ML) who want to filter/join/aggregate with SQL rather than do point lookups. The design moves I'd make are the ones I already make in Hive:
- **Partition by date** (`DATE(timestamp)`) - in Hive this is a physical directory-per-day in HDFS that lets a query skip irrelevant partitions entirely. Searching confirms BigQuery's partitioning gives the same scan-pruning effect, but manages it internally rather than exposing partitions as literal storage paths the way Hive does over HDFS - partition pruning happens inside the query planner against metadata, not via directory listing.
- **Cluster by `service`, `event_type`** - the equivalent of Hive's `CLUSTERED BY`/bucketing. A search on BigQuery clustering confirms it works the same way in effect: columns that are filtered on constantly get physically co-located into sorted blocks, so a filtered query reads fewer blocks.
- **Keep the raw, untouched payload alongside the typed columns** - in a Hive pipeline I'd keep a raw/bronze table untouched by cleaning logic, separate from a cleaned/silver table, specifically so a bad cleaning rule can be fixed and replayed without needing to re-ingest from source. I'd carry that same raw-vs-cleaned table split into BigQuery.

**Aggregated metrics -> Bigtable, with a copy also in BigQuery.** This is where the Cassandra experience maps almost directly:
- Bigtable, like Cassandra, has no joins and no ad hoc secondary indexes - the row key *is* the query plan. So I'd design the row key the same way I'd design a Cassandra partition key: `service` first (low-cardinality, ~5 values, spreads writes across nodes/tablets instead of hotspotting), then a timestamp component (reversed/padded, the way you'd reverse-order a clustering column in Cassandra to get "most recent first" cheaply on a range scan) so "give me checkout's last 10 minutes" is a short, ordered range scan rather than a full-table operation.
- This is for the low-latency *serving* path - a live ops dashboard or alert polling "what's the error rate right now," the same job Cassandra does for me today: point/range reads under tight latency, not analytical queries.
- For the *analytical* path - "how has p95 latency trended this quarter" - that's a Hive-shaped question again, so the same aggregation output also gets written to a BigQuery table. Same data, two destinations, because the access pattern differs, not the data.
- One real difference from Cassandra: searching Google's Bigtable documentation confirms single-row read/write operations are strongly consistent and atomic by default, with no tunable consistency level to choose between - unlike Cassandra, where I pick a consistency level (eventual vs. quorum) per query. One less dial to reason about.

**Schema evolution.** Two tiers, same as I'd handle it in Hive/Cassandra:
- *Additive* (a new nullable field) is safe and routine - in Hive, adding a column to an Avro/Parquet-backed table with schema evolution enabled doesn't require touching old data; in Cassandra, adding a column (or just writing a new column into a wide row) doesn't require a migration either, since rows are sparse by nature. Searching confirms both GCP equivalents work the same way: BigQuery supports adding a nullable column to an existing table with no backfill or rewrite required, and Bigtable's column families are sparse by design, so writing a new column for the first time needs no schema migration at all.
- *Breaking* changes (rename, retype, change meaning) aren't safe in place in any of these systems, including the ones I know well - I'd handle it the way I'd handle a breaking Avro schema change in a Hive/Kafka pipeline: version the schema explicitly (`schema_version` on every event), write the new shape alongside the old during a transition window, and branch the cleaning logic on that version field - which is the same pattern `clean_record` already uses to tolerate the `extra_field` drift found in the sample data, just made explicit with a version number instead of silently ignoring unknowns.

## Part 3 - Data API

A small FastAPI app (`api/`) serving the metrics Part 1 produces:

- `GET /health` - liveness check.
- `GET /metrics?service=&from=&to=` - aggregated metrics, optionally filtered by exact service name and/or an inclusive `minute` range (ISO-8601 timestamps). Any filter left out is simply not applied. An unmatched filter returns `200` with an empty list, not `404` - it's a collection endpoint, and "no rows matched" isn't the same as "this resource doesn't exist." `from` after `to` returns `400`.

The API reads `output/metrics.csv` once at process startup (see `api/repository.py`) rather than per-request. Route handlers never see the CSV directly - they depend on `MetricsRepository`, so the data source can later become the Bigtable/BigQuery-backed store described in Part 2 without changing `api/main.py`. The Pydantic response schema (`api/schemas.py`) is kept separate from the pipeline's internal `MetricRow` dataclass on purpose: one is the wire contract to API clients, the other is the pipeline's internal representation, and they're allowed to change independently.

## Part 4 - Code quality & reliability

**What I'd monitor in production:**
- **Reject rate, broken down by reason code** - `run_pipeline.py` already logs this breakdown (`missing_or_invalid_timestamp: 30`, etc.). In production I'd ship that as a metric per reason, not just a log line, because a sudden spike in one specific reason (e.g. `missing_service` jumping from near-zero to 40%) is usually the first signal that an upstream producer changed its schema - exactly the kind of thing the reason-coded reject log was built to surface.
- **Null-rate trends in `cleaned_events`** for `latency_ms`/`status_code`, not just raw counts - a creeping increase means something upstream is degrading even though no record is being outright rejected.
- **The dedup store's size over time** (`InMemorySeenStore._ids`) - it's unbounded by design (documented in `dedupe.py`), so its growth curve is itself a thing worth watching: it should track roughly with unique `event_id` volume, and if it doesn't plateau relative to expected traffic, that's the memory-leak warning sign before it becomes an outage.
- **API request rate/latency/error rate** on `/metrics` (standard RED metrics), plus **data staleness** - how long since `output/metrics.csv` was last loaded - since the API currently reads it once at startup with no refresh, that staleness number would currently just grow unbounded until a restart.

**What could break as data volume grows** (the honest list, several of which are already flagged in code comments):
- `run_pipeline.py` materializes `clean_events` as a full in-memory list to feed two sinks (the cleaned-events file and the aggregator) from one generator - the one place the "fully streaming" design breaks down, called out explicitly in that file. At 10GB this is the first thing to fail.
- The in-memory dedup set has no TTL or bound - on a long-running, high-volume stream it grows forever. `dedupe.py`'s `SeenStore` protocol exists specifically so this can be swapped for a time-bounded store without touching the dedup logic itself.
- `aggregate_events` buffers every event before yielding any metric row, because a late-arriving record could still belong to an already-seen minute. That's fine for a finite batch file; it's the wrong model for a truly continuous stream, which needs real watermark-based window closing (what Beam provides) instead.
- Single `metrics.csv`/`cleaned_events.jsonl` output files assume the whole result fits in one serial write/read. At real scale this needs sharded output, the way Beam/Dataflow writes multiple part-files automatically.
- Everything runs single-threaded in one process - wall-clock time scales linearly with input size, with no way to add more machines, unlike a real Dataflow job which parallelizes across workers automatically.

**What I'd improve next with more time:**
1. Fix the `run_pipeline.py` fan-out: drive the cleaned-events write and the aggregation accumulator from a single pass over the stream (write each record to disk *and* update an in-memory accumulator in the same loop iteration) instead of collecting a list first - removes the one non-streaming step.
2. Add a `schema_version` field to the event contract (discussed in Part 2 but not implemented) so `clean_record` has an explicit branch point for handling breaking upstream changes, rather than only tolerating additive drift the way it does today via the ignored `extra_field`.