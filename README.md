# LoL Ranked Data Pipeline

End-to-end data engineering pipeline that ingests League of Legends ranked match data from the Riot Games API and processes it through a Medallion Architecture (Bronze → Silver → Gold) on Databricks, producing analytics-ready tables such as champion win rates by lane, patch, and game duration.

## Overview

The pipeline pulls match and timeline data for Challenger-tier players in EUW ranked solo queue, lands the raw JSON in a Databricks Volume, and transforms it through three progressively refined layers using Delta Lake. The final Gold layer answers concrete analytical questions — for example, **"What is each Top Lane champion's win rate in patch 16.11, split by early vs. late game?"**

The pipeline is idempotent: it tracks which matches have already been processed and only fetches new ones on each run. Bronze and Silver layers use Delta `MERGE` to avoid duplicate inserts; the Gold layer uses `CREATE OR REPLACE TABLE` for full refresh on each run.

## Architecture

```
Riot Games API
      │
      ▼
┌─────────────┐
│   Ingestion  │  Python script: fetches player list → match IDs → match & timeline JSON
└─────────────┘
      │
      ▼
┌─────────────┐
│   Bronze     │  Raw JSON landed in a Databricks Volume, then loaded into Delta tables
│              │  (b_matches, b_timelines) via MERGE — append-only, deduplicated
└─────────────┘
      │
      ▼
┌─────────────┐
│   Silver     │  Cleaned, flattened, typed tables: general_info (one row per match),
│              │  player_info (one row per player per match, via EXPLODE).
│              │  Remakes filtered out (games < 14 min excluded).
└─────────────┘
      │
      ▼
┌─────────────┐
│   Gold       │  Aggregated analytics tables, e.g. Winrate_TopLane_Patch_26_11:
│              │  win rate by champion, split by early game (≤25 min) vs. late game (>25 min)
└─────────────┘
```

**Pipeline flow (ingestion script):**
1. `get_players` — fetch the Challenger-tier player list for the configured queue/tier/division
2. `get_matches_id` — for each player's PUUID, fetch their recent ranked match IDs
3. `get_processed_matches` — check which matches are already stored in Databricks
4. `fetch_and_upload_matches` — fetch only the new matches' full data + timeline, upload both as JSON to the Bronze Volume
5. Rate limiting and transient errors are handled by `safe_request`, which backs off on HTTP 429 and retries

## Tech Stack

- **Python** — ingestion (`requests`, Databricks SDK)
- **Databricks** — compute, orchestration, Unity Catalog
- **Delta Lake** — storage format, `MERGE` for deduplication in Bronze and Silver layers
- **Spark SQL** — all Bronze → Silver → Gold transformations
- **Riot Games API** — `league-exp`, `match-v5` endpoints

## Setup

1. Clone the repo
   ```bash
   git clone https://github.com/FayeValentine21/LoL.git
   cd LoL
   ```
2. Copy `.env.example` to `.env` and fill in your credentials:
   ```
   RIOT_API_KEY=your_riot_api_key
   DATABRICKS_HOST=your_databricks_host
   DATABRICKS_TOKEN=your_databricks_token
   ```
3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the ingestion script to fetch new matches and land them in Bronze:
```bash
python ingestion.py
```

Then run the Bronze, Silver, and Gold notebooks in order on a Databricks cluster (or schedule them as a Databricks Job) to refresh each layer.

The pipeline is fully parameterizable — `queue`, `tier`, `division`, and `queue_id` in `main()` can be changed to target other regions, ranks, or queue types.

## Example Output

`Winrate_TopLane_Patch_26_11` — Top Lane champion win rates for patch 16.11, minimum 20 games, split by game length:

| champion | games | winrate | early_game_wr | late_game_wr | patch |
|----------|-------|---------|----------------|---------------|--------|
| ...      | ...   | ...     | ...            | ...           | 16.11.x |

This is one example of many possible Gold tables the Silver layer can support — the same `player_info` / `general_info` tables can be aggregated by role, champion, item build, or any other dimension present in the match data.

## Future Improvements

- Partition Bronze tables by `game_version` for better query performance
- Auto Loader for incremental JSON ingestion (replacing the manual batch read)
- Airflow (or Databricks Workflows) orchestration to schedule ingestion + transformation end-to-end
- `MERGE` logic on Gold layer — currently uses `CREATE OR REPLACE TABLE` (full refresh); incremental MERGE would avoid reprocessing the entire history on each run
- Unit tests for the transformation logic
- Parameterize the Gold layer queries (currently one table per patch) into a reusable, patch-agnostic view

## Project Structure

```
LoL/
├── ingestion.py          # Riot API → Databricks Bronze Volume
├── bronze.sql            # Bronze: raw JSON → Delta tables (MERGE)
├── silver.sql            # Silver: cleaned, flattened, remake-filtered tables
├── gold.sql               # Gold: aggregated analytics tables
├── setup.sql              # Catalog/schema creation
├── requirements.txt
├── .env.example
└── .gitignore
```
