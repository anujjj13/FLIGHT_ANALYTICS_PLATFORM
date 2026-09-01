# Runbook — Real-Time Flight Analytics Platform

**Purpose of this document:** if this project is handed to you and you've never touched it before, this is everything you need to get it running, verify it's working, and know what to do when something breaks. Read this top to bottom once before touching anything.

---

## 1. What This System Does

```
OpenSky API (live aircraft positions)
   -> Python Producer (OAuth2 auth, confluent-kafka)
   -> Azure Event Hubs (Kafka-compatible endpoint)
   -> Databricks Spark Structured Streaming
        01_eventhub_bronze_ingestion.ipynb  -> Bronze Delta table (raw, untransformed)
        02_silver_layer.ipynb               -> Silver Delta table + dead-letter table
        03_gold_layer.ipynb                 -> Gold Delta tables:
                                                  - flight_kpis
                                                  - streaming_health
                                                  - longest_flights
   -> Databricks Dashboard (reads Gold tables)
```

A scheduled **Databricks Job** runs the three notebooks in order (01 → 02 → 03) so this isn't something that needs to be triggered by hand day-to-day. The producer is the one piece that currently needs to be running somewhere continuously (see Section 6).

---

## 2. Prerequisites — Access You Need Before Starting

Get these sorted first; nothing below will work without them.

| What | Where to get it | Notes |
|---|---|---|
| OpenSky Network account + API client credentials | https://opensky-network.org/my-opensky/account | Free account; register an API client under "API Client" to get a Client ID/Secret |
| Access to the Azure Event Hubs namespace | Ask the project owner / your Azure admin | You need the **Listen** policy connection string for Databricks, and the **Send** policy connection string for the producer — these are different strings, do not mix them up (see Section 8) |
| Access to the Databricks workspace | Ask the project owner / workspace admin | You need permission to run notebooks, create/edit Jobs, and read/write the `flight_analytics` Unity Catalog Volume |
| Python 3.10+ and `pip` | — | For running the producer locally |
| Docker Desktop (optional) | — | Only needed if you want to test the producer against a local Kafka broker instead of Event Hubs directly — not required for production |

---

## 3. Repo Setup (Local Machine)

```bash
git clone <repo-url>
cd flight-analytics-platform

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# now edit .env and fill in:
#   OPENSKY_CLIENT_ID
#   OPENSKY_CLIENT_SECRET
#   EVENTHUB_CONNECTION_STRING   <- the SEND policy connection string
#   EVENTHUB_NAMESPACE
#   EVENTHUB_NAME
```

Load the `.env` file into your shell before running anything:

```bash
export $(cat .env | xargs)        # macOS/Linux
```

**Never commit `.env`.** It's already in `.gitignore` — double-check `git status` doesn't show it before your first commit.

---

## 4. Running the Producer

```bash
python -m producer.kafka_producer
```

Expected output: a log line roughly every 10 seconds like:

```
2026-09-01 10:03:12 | INFO | producer.kafka_producer | Poll cycle #4 completed: published=118 skipped=2 total_published=402
```

If you see this repeating, the producer is successfully fetching from OpenSky and publishing to Event Hubs. Stop it cleanly with `Ctrl+C` — it will flush any in-flight messages before exiting rather than dropping them.

**If you see auth errors immediately:** double-check `.env` is actually loaded into your shell (`echo $EVENTHUB_CONNECTION_STRING` should print something), and that you're using the **Send** policy connection string, not Listen.

The producer needs to run continuously for the pipeline to have live data. For anything beyond local testing, run it as a background service (systemd, a scheduled VM, a container, etc.) rather than leaving a terminal open — this project doesn't currently include that deployment wrapper, so you'll need to choose one appropriate for your environment.

---

## 5. Databricks Setup (One-Time)

### 5.1 Upload the notebooks
Import the three notebooks from `notebooks/` into your Databricks workspace, keeping them in the same order/folder if possible — `01_eventhub_bronze_ingestion`, `02_silver_layer`, `03_gold_layer`.

### 5.2 Set up the secret scope
The Bronze notebook reads the Event Hubs **Listen** connection string from a Databricks secret scope — it is never hardcoded. Set this up once:

```bash
databricks secrets create-scope flight-analytics
databricks secrets put-secret flight-analytics eventhub-listen-conn-str
# paste the LISTEN policy connection string when prompted
```

### 5.3 Confirm Unity Catalog Volume paths
All three notebooks read/write under:

```
/Volumes/flight_analytics/streaming/flight_data/
    bronze/
    silver/
    silver_deadletter/
    gold/flight_kpis/
    gold/streaming_health/
    gold/longest_flights/
    checkpoints/
        bronze/
        silver/
        silver_deadletter/
        gold_flight_kpis/
        gold_streaming_health/
        gold_longest_flights/
```

If this Volume doesn't exist yet in your workspace/catalog, create it before running anything — the notebooks assume it's already there.

---

## 6. Running the Pipeline

### Option A — Scheduled Databricks Job (normal operation)
The pipeline runs via a Databricks Job with three tasks in sequence: Bronze → Silver → Gold, each task depending on the previous one completing. Find it in the Databricks Jobs UI (search for the flight analytics job), check its schedule/trigger, and confirm it's enabled. This is the "just let it run" path — no manual steps needed once the producer is feeding data in.

### Option B — Manual run (testing / first run / debugging)
Run the three notebooks in order, waiting for each to finish before starting the next:

1. **`01_eventhub_bronze_ingestion.ipynb`** — reads from Event Hubs, writes to Bronze. Uses `trigger(availableNow=True)`, so it processes everything currently available and then stops (it does not run forever) — this is intentional for controlled/testable runs. Confirm the final cell shows rows landing in Bronze.
2. **`02_silver_layer.ipynb`** — reads Bronze, writes Silver + dead-letter. Check the sanity-check cells at the bottom: a reasonable Silver row count, and a dead-letter sample showing *why* each rejected record failed (which check it failed).
3. **`03_gold_layer.ipynb`** — reads Silver, writes all three Gold tables. Check each of the three sanity-check cells at the bottom.

**Both paths are safe to re-run.** Silver and two of the three Gold tables use Delta `MERGE` (idempotent upserts) — running twice does not create duplicate rows. The dead-letter table and `streaming_health` are append-only by design (every rejected record and every batch's metrics are meant to accumulate).

---

## 7. Verifying the Pipeline Is Healthy

Quick checks, in order, if you're not sure things are working:

```python
# Row counts at each layer - should be non-zero and growing over successive runs
spark.read.format("delta").load("/Volumes/flight_analytics/streaming/flight_data/bronze").count()
spark.read.format("delta").load("/Volumes/flight_analytics/streaming/flight_data/silver").count()
spark.read.format("delta").load("/Volumes/flight_analytics/streaming/flight_data/gold/flight_kpis").count()
```

```python
# Dead-letter rate - if this is climbing as a % of total, something upstream changed
# (e.g. OpenSky altered its response format) and is worth investigating
dl = spark.read.format("delta").load("/Volumes/flight_analytics/streaming/flight_data/silver_deadletter")
dl.count()
```

```python
# Pipeline throughput proxy (see Section 9 - this is NOT true Kafka consumer lag)
display(
    spark.read.format("delta")
    .load("/Volumes/flight_analytics/streaming/flight_data/gold/streaming_health")
    .orderBy("recorded_at", ascending=False)
    .limit(10)
)
```

The dashboard itself is the fastest sanity check for a non-technical handoff — if it's showing recent, moving numbers, the pipeline is healthy end to end.

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Producer fails to authenticate to Event Hubs | Using the Listen connection string instead of Send (or vice versa) | Producer needs **Send**; Databricks Bronze notebook needs **Listen**. These are different strings — check which policy you copied. |
| Bronze notebook fails to auth | Same issue, reversed | Confirm the secret scope has the **Listen** string, not Send. |
| `PERSIST TABLE is disallowed` or similar Serverless error | Databricks Serverless compute doesn't support manual DataFrame caching | Don't add `.persist()`/`.cache()` calls to the Silver/Gold `foreachBatch` writers — this was deliberately removed already; if you're extending the code, avoid reintroducing it. |
| `ProcessingTime trigger not supported` | Serverless compute limitation | Use `trigger(availableNow=True)` instead of `trigger(processingTime=...)`. |
| Notebook fails to write, checkpoint/DBFS path errors | Old code pointing at the disabled public DBFS root | Confirm all paths point into the Unity Catalog Volume (`/Volumes/flight_analytics/...`), not `/dbfs/...`. |
| `IndexError` or missing-field errors from OpenSky data | Live API occasionally returns 17 fields instead of 18 (drops `category`) | Already handled in `producer/opensky_client.py` (`_parse_state_vector` pads short arrays) — if you see this error, something upstream changed and needs re-checking, not a config fix. |
| Dead-letter table growing fast | A real upstream data quality issue, or a DQ rule is miscalibrated | Check `silver_deadletter`'s `chk_*` columns to see which specific rule is failing most, then decide whether the rule or the data is wrong. |
| Producer runs but no new rows in Bronze | Producer isn't actually running continuously, or is pointed at the wrong Event Hub/topic | Confirm the producer process is still alive; confirm `EVENTHUB_NAME`/topic in `.env` and `config.yaml` match what the Bronze notebook subscribes to. |

---

## 9. Known Limitations (Don't Be Surprised By These)

- **`streaming_health` is a throughput proxy, not true Kafka consumer lag.** Offset-based lag isn't reliably available from inside `foreachBatch` over the Event Hubs Kafka-compatible endpoint. Batch duration and rows/sec are used instead — a growing batch duration or falling throughput is a fair practical stand-in, but if anyone asks for "consumer lag" specifically, be clear this isn't that.
- **No automated alerting yet** if a scheduled Job run fails — currently requires someone to notice in the Databricks Jobs UI. Worth adding job-run notifications.
- **No CI/CD pipeline yet** — tests exist (`tests/`) but aren't wired into a GitHub Actions (or equivalent) workflow that gates merges.
- **The producer has no managed deployment** — it's a script you run, not a deployed service. If it's only ever run from someone's laptop, the pipeline goes stale the moment that laptop is off.

---

## 10. Day-2 Operations

**Change the geographic scope:** edit `opensky.bounding_box` in `config/config.yaml` (currently set to India). Set any value to `null` to disable bounding-box filtering — not recommended, this pulls global data and is a large volume increase.

**Change how often OpenSky is polled:** edit `opensky.poll_interval_seconds` in `config/config.yaml`. OpenSky's own data typically refreshes every 5–15s server-side, so polling much faster than that wastes API credits without getting fresher data.

**Rotate a credential:** update it in Azure/OpenSky first, then update the corresponding `.env` value (producer) or Databricks secret (Bronze notebook — `databricks secrets put-secret flight-analytics eventhub-listen-conn-str` again). Restart the producer process after rotating its side.

**Reset a stuck checkpoint:** each streaming query's checkpoint is independent (Bronze, Silver, and each of the three Gold writers). If one query is stuck or behaving unexpectedly, you can delete just that query's checkpoint folder under `/Volumes/flight_analytics/streaming/flight_data/checkpoints/<name>/` to force it to reprocess from the earliest available offset in its source — do this deliberately, since it means reprocessing history, not routine maintenance.

---

## 11. Ownership / Contacts

*(Fill in before handing this off — this is the one section that can't be inferred from the code.)*

- Project owner:
- Azure Event Hubs namespace admin:
- Databricks workspace admin:
- Where the scheduled Job's failure notifications should go:
