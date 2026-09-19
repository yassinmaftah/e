# s-MeteoRisk — Project Analysis

This document provides a structured analysis of the **s-MeteoRisk** project: a meteorological risk-monitoring system for Moroccan cities. It covers the whole codebase, summarizes every function, and highlights concrete optimization and refactoring opportunities.

---

## 1. Project Overview

### What it does

s-MeteoRisk is an end-to-end **ETL + analytics** platform. Every day it:

1. Pulls a 7-day weather forecast from the [Open-Meteo API](https://open-meteo.com) for **120 Moroccan cities** (listed in `bronze/ma.csv`).
2. Cleans the raw JSON, normalizes it into a tabular dataset, and engineers **risk scores** (wind / rain / temperature) for each city-day.
3. Loads the enriched data into a **PostgreSQL** database.
4. Exposes the results through an interactive **Streamlit dashboard** and static SQL analysis queries.

### Architecture (Medallion / Bronze–Silver–Gold)

```
                        ┌───────────────────────────┐
                        │   Open-Meteo API (HTTPS)  │
                        └─────────────┬─────────────┘
                                      │
                         Apache Airflow DAG: "s_meteorisk_daily_update"
                                      │  schedule: 0 2 * * *
              ┌───────────────────────┼───────────────────────┐
              │ 1. Extract            │ 2. Transform           │ 3. Load
              ▼                       ▼                        ▼
   ┌───────────────────┐   ┌───────────────────────┐   ┌──────────────────┐
   │  BRONZE (raw)     │   │  SILVER (cleaned)     │   │  GOLD (Postgres) │
   │  bronze/data01.json│──▶│  silver/df_silver.csv │──▶│ cities           │
   │  (JSON per city)  │   │  + risk_score_total   │   │ weather_risks    │
   └───────────────────┘   └───────────────────────┘   └──────────────────┘
                                                                  │
                                   ┌──────────────────────────────┤
                                   ▼                              ▼
                    ┌────────────────────────┐         ┌────────────────────┐
                    │ Streamlit dashboard    │         │ SQL analysis       │
                    │  dashboard/app.py      │         │   gold/analysis.sql │
                    └────────────────────────┘         └────────────────────┘
```

- **Bronze** (`extraction/extract_bronze.py`): fetches raw forecast JSON per city and persists it to `bronze/data01.json`.
- **Silver** (`transformation/json_to_df.py` + `transformation/clean_silver.py`): JSON → flat DataFrame, date/type cleaning, forward-fill, dedup, merge with city coordinates, then engineering of `risk_wind`, `risk_rain`, `risk_temp`, `risk_score_total`.
- **Gold** (`gold/create_db.py` + `gold/insert_data.py`): SQLAlchemy ORM model (`cities`, `weather_risks` tables) and incremental upsert of the silver CSV.
- **Consumption**: `dashboard/app.py` (Streamlit KPIs & charts) and `gold/analysis.sql` (top 5 cities by temperature / precipitation / risk, max-risk periods).

### Orchestration

`dags/meteorisk_pipeline.py` defines the single DAG `s_meteorisk_daily_update` with three `PythonOperator` tasks (extract → transform → load). Airflow runs in Docker (`docker-compose.yml`, based on `apache/airflow:2.7.1`), with a shared volume mounted to `/opt/airflow/`; PostgreSQL runs as the `meteorisk_db` service (`postgres:16`). The `db` schema is created either via `python gold/create_db.py` or inside the DAG image.

### Data flow detail

1. `load_cites()` reads lat/lng from `bronze/ma.csv`.
2. `fetch_data_with_API()` calls Open-Meteo sequentially (with a 100 ms delay between calls) and tags each response with a `city`.
3. `save_to_bronze()` dumps the list of responses to `bronze/data01.json`.
4. `json_to_dataframe()` expands each city's `daily` arrays (7 days) into one row per city-day and writes an early CSV.
5. `clean_data()` fixes types/dates, fillna + ffill, drops duplicates, merges coordinates, computes risk features and re-writes `silver/df_silver.csv`.
6. `insert_silver_data()` maps each silver row to category strings, creates missing `City` rows, and creates/updates `WeatherRisk` rows.
7. `dashboard/app.py` `load_data()` joins both tables and the Streamlit UI applies client-side filters.

### Notable current-state observations

- **Directory as pipeline layers**: the three Airflow tasks map 1:1 to the `bronze/`, `silver/`, `gold/` folders (a small-scale medallion architecture).
- **Risk model**: `risk_score_total` is capped at 100. Wind gusts >85 km/h add 50, >60 add 30, >40 add 15; rain probability adds `prob*0.3`; extremes >40 °C or <2 °C add 20.
- **weak points** (detailed in section 3): relative file paths, sequential API calls, duplicated CSV writes, an N+1 query pattern in the load step, hardcoded DB credentials, and dead/buggy date-fix logic.

---

## 2. Functions Summary

### `extraction/extract_bronze.py`

| Function | Role | Inputs | Outputs |
|---|---|---|---|
| `load_cites(file_path)` | Load the list of Moroccan cities to monitor. | `file_path: str` — path to a CSV with `city`, `lat`, `lng` columns (e.g. `bronze/ma.csv`) | `pandas.DataFrame` subset `[city, lat, lng]` |
| `fetch_data_with_API(cities)` | Query the Open-Meteo forecast API for each city and collect responses. | `cities: DataFrame` of `[city, lat, lng]` | `list[dict]` — one forecast payload per city, augmented with a `city` key |
| `save_to_bronze(data, file_path)` | Persist raw API payloads to the bronze layer as pretty-printed JSON. | `data: list` of API responses; `file_path: str` | Writes `file_path`; creates parent dirs; `None` (prints `"Done"`) |
| `x()` | Pipeline entry point: `load_cites → fetch_data_with_API → save_to_bronze`, wiring bronze paths. | none | Writes `bronze/data01.json`; `None` |

### `transformation/json_to_df.py`

| Function | Role | Inputs | Outputs |
|---|---|---|---|
| `json_to_dataframe(data_file)` | Normalize the per-city JSON (each with nested `daily` arrays) into a flat DataFrame of one row per city-day. | `data_file: str` — path to bronze JSON | `pandas.DataFrame` with columns `city, date, temperature_2m_max, temperature_2m_min, precipitation_probability_max, wind_speed_10m_max, wind_gusts_10m_max, weather_code`; also writes `silver/df_silver.csv` |

### `transformation/clean_silver.py`

| Function | Role | Inputs | Outputs |
|---|---|---|---|
| `clean_data(df)` | Full silver cleaning pass: fill NaN, parse dates, forward-fill weather columns, drop duplicates, merge coordinates, compute risk features, persist CSV. | `df: DataFrame` (from `json_to_dataframe`) | Mutated copy of `df` with `lat, lng, risk_wind, risk_rain, risk_temp, risk_score_total`; writes `silver/df_silver.csv` |
| `fix_mission_day(date)` | Attempted date correction — returns the input date + 1 day. | `date: Timestamp` | `Timestamp` (1 day later). **Currently produces no meaningful effect** (see §3.4) |
| `calculate_risk_features(df)` | Feature engineering of the three sub-risk scores and the capped total. | `df: DataFrame` (silver) | Same `df` with new columns `risk_wind`, `risk_rain`, `risk_temp`, `risk_score_total` (rounded to 2 dp) |
| `x2()` | Pipeline entry point: `json_to_dataframe('bronze/data01.json') → clean_data(df)`. | none | Writes `silver/df_silver.csv`; `None` |

### `gold/create_db.py`

| Function | Role | Inputs | Outputs |
|---|---|---|---|
| `engine` (module-level) | SQLAlchemy engine bound to PostgreSQL (`meteorisk_db`). | DB config (hardcoded: user/pass/host/db) | `Engine` (driver: `psycopg2`-style via `pg8000`) |
| `City(Base)` | ORM model → table `cities` (`id`, `name` unique, `lat`, `lng`). | — | Declarative model |
| `WeatherRisk(Base)` | ORM model → table `weather_risks` (`id`, FK `city_id`, `date`, `temp_max`, `precip_max`, `wind_gusts`, `risk_total`, 3 category columns). | — | Declarative model |
| `Base.metadata.create_all(engine)` | `__main__` block creating both tables if missing. | engine | Creates tables; prints confirmation |

### `gold/insert_data.py`

| Function | Role | Inputs | Outputs |
|---|---|---|---|
| `get_temp_cat(temp)` | Map max temperature to a French label. | `temp: float` | `'Froid'` (<15), `'Modéré'` (≤25), `'Chaud'` (>25) |
| `get_precip_cat(prob)` | Map precipitation probability to a label. | `prob: float` | `'Faible'` (<20), `'Moyenne'` (≤60), `'Forte'` (>60) |
| `get_wind_cat(wind)` | Map wind gust speed to a label. | `wind: float` | `'Calme'` (<20), `'Modéré'` (≤50), `'Fort'` (>50) |
| `insert_silver_data()` | Read `silver/df_silver.csv`, apply the category functions, insert/update `City` and `WeatherRisk` rows (upsert by name and by city+date). | none (reads CSV, opens DB session) | Writes to PostgreSQL; prints progress; `None` |
| `x3()` | Pipeline entry point wrapping `insert_silver_data()`. | none | `None` |

### `dashboard/app.py`

| Function | Role | Inputs | Outputs |
|---|---|---|---|
| `load_data()` (cached) | Run the `cities ⋈ weather_risks` join and return a DataFrame for the UI. | none | `DataFrame` (city, date, temp_max, precip_max, wind_gusts, risk_total, three categories) |
| *(script body)* | Builds sidebar filters, KPI metric cards, and three charts (top temp per city, top precip per city, risk scatter). | `df` from `load_data()` | Rendered Streamlit page |

### `dags/meteorisk_pipeline.py`

| Object | Role | Notes |
|---|---|---|
| `default_args` dict | Common task config (owner, retries=3, retry_delay=5 min, start 2026-09-18). | `schedule_interval='0 2 * * *'` (see §3.7) |
| `DAG('s_meteorisk_daily_update')` | Orchestration graph with 3 `PythonOperator` tasks. | Tasks call the poorly-named callables `x`, `x2`, `x3`; order `extract_task >> transform_task >> load_task` |

---

## 3. Optimization & Refactoring

This section lists concrete, high-impact improvements. Each entry names the file, explains the problem, and gives a rewritten block.

### 3.1 Fragile relative paths → `Path(__file__)`

**File:** `extraction/extract_bronze.py:64-69`, and the same pattern in `transformation/clean_silver.py:56`, `transformation/json_to_df.py:52`, `gold/insert_data.py:33`.

All paths like `'bronze/ma.csv'` are resolved against the **current working directory**. Inside Airflow this happens to work because the repo is mounted at `/opt/airflow/`, but it breaks from any other CWD (e.g. running `python dashboard/app.py`, tests, or a scheduled runner with a different home). Use the module location instead of the CWD:

```python
# extraction/extract_bronze.py
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CITIES_FILE = PROJECT_ROOT / "bronze" / "ma.csv"
BRONZE_OUT  = PROJECT_ROOT / "bronze" / "data01.json"

def x():
    cities = load_cites(CITIES_FILE)
    new_data = fetch_data_with_API(cities)
    save_to_bronze(new_data, BRONZE_OUT)
```

Apply the same `PROJECT_ROOT` pattern in `clean_data` (`silver/df_silver.csv`), `json_to_dataframe` and `insert_silver_data`.

### 3.2 Sequential API calls → parallel fetch

**File:** `extraction/extract_bronze.py:15-49`.

120 cities are fetched one-by-one with a mandatory `time.sleep(0.1)`, making the extract step take tens of seconds and dominate the pipeline. The API is stateless and rate-tolerant — fetching in parallel with a small thread pool is a near-free win:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL_API = "https://api.open-meteo.com/v1/forecast"
DAILY_COLS = [
    "temperature_2m_max", "temperature_2m_min", "precipitation_sum",
    "precipitation_probability_max", "wind_speed_10m_max",
    "wind_gusts_10m_max", "weather_code",
]

def fetch_one(row):
    params = {
        "latitude": row["lat"], "longitude": row["lng"],
        "daily": DAILY_COLS, "timezone": "auto",
    }
    try:
        resp = requests.get(BASE_URL_API, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        payload["city"] = row["city"]
        return payload
    except requests.exceptions.RequestException as e:
        print(f"city: {row['city']}, error: {e}")
        return None

def fetch_data_with_API(cities):
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_one, row): row["city"] for _, row in cities.iterrows()}
        results = [f.result() for f in as_completed(futures)]
    return [r for r in results if r is not None]
```

Notes: drop `time.sleep`, remove the per-city print (or move it to debug logging). If a city succeeds most days, the parallel version collapses ~12 s of I/O waits to a couple of seconds.

### 3.3 Vectorize JSON → DataFrame

**File:** `transformation/json_to_df.py:4-38`.

The nested `for city → for day → dict(...).get(key, [None]*num_days)[i]` loop creates a fresh `[None]*num_days` list on **every cell access** and appends row-by-row — O(cells) wasted allocations. Normalize with `json_normalize` and one `concat`, then melt the per-day column arrays into rows:

```python
import json
import pandas as pd

FORECAST_COLS = [
    "temperature_2m_max", "temperature_2m_min",
    "precipitation_probability_max",
    "wind_speed_10m_max", "wind_gusts_10m_max", "weather_code",
]

def json_to_dataframe(data_file):
    with open(data_file, "r", encoding="utf-8") as f:
        records = json.load(f)

    frames = []
    for rec in records:
        daily = rec.get("daily", {})
        base = pd.DataFrame({"city": rec.get("city"), "date": daily.get("time", [])})
        for col in FORECAST_COLS:
            base[col] = daily.get(col, [None] * len(base))
        frames.append(base)

    return pd.concat(frames, ignore_index=True)
```

This also removes the premature `df.to_csv('silver/df_silver.csv')` write (see §3.4), keeping the transform layer's I/O in a single place.

### 3.4 Clean up dead / misleading logic in `clean_data`

**File:** `transformation/clean_silver.py:7-27`.

Three concrete problems:

1. **`fix_mission_day` does nothing useful.** `df.fillna(0, inplace=True)` runs first, so there are no more NaN dates by the time `fillna(expected_dates)` executes — and if a date *were* NaN it would become the integer `0`, which `pd.to_datetime` coerces to `1970-01-01`, i.e. garbage. The shifted "+1 day" logic is neither applied nor documented (it does not explain how the silver dates end up offset from the bronze dates).
2. **`fillna(0)` before `ffill()`** defeats the purpose of `ffill`: weather columns are first zero-filled, so the subsequent `ffill()` is a no-op and missing values silently become `0.0` (bad for risk math and DB stats).
3. **Double CSV write.** `json_to_dataframe` also writes `silver/df_silver.csv`, then `clean_data` overwrites it. Only one write should exist.

Cleaner version:

```python
def clean_data(df):
    df = df.copy()

    # keep dates honest: ffill real values first, drop irrecoverable rows
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    weather_cols = [
        "temperature_2m_max", "temperature_2m_min",
        "precipitation_probability_max", "wind_speed_10m_max",
        "wind_gusts_10m_max", "weather_code",
    ]
    df[weather_cols] = df[weather_cols].ffill()
    df = df.dropna(subset=["date"]).drop_duplicates()

    cities_df = pd.read_csv(PROJECT_ROOT / "bronze" / "ma.csv")
    df = pd.merge(df, cities_df[["city", "lat", "lng"]], on="city", how="left")
    df = calculate_risk_features(df)
    df.to_csv(PROJECT_ROOT / "silver" / "df_silver.csv", index=False)
    return df
```

If the +1-day offset is actually intended (matching forecast issue date to +1 horizon), apply it **explicitly and once**, after parsing:

```python
df["date"] = pd.to_datetime(df["date"], errors="coerce") + pd.Timedelta(days=1)
```

…rather than through a `shift().apply(...)` chain whose placement achieves nothing. Delete `fix_mission_day` unless you restore it for this purpose.

### 3.5 Eliminate the N+1 query pattern in the load step

**File:** `gold/insert_data.py:26-93`.

The current flow emits one `City` query per unique city, **and then one `WeatherRisk` query per row** (840 rows at the current data size) — two nested query storms per run. The loop also mutates existing ORM objects one-by-one instead of batching.

Refactor to fetch the mapping once, batch the inserts, and let the DB dedupe:

```python
from sqlalchemy.dialects.postgresql import insert

def upsert_city_ids(session, df):
    """One round-trip to map existing city names -> ids."""
    cities = df[["city", "lat", "lng"]].drop_duplicates()
    existing = {
        c.name: c.id for c in
        session.query(City).filter(City.name.in_(cities["city"].tolist())).all()
    }
    for _, c in cities.iterrows():
        if c["city"] not in existing:
            new_city = City(name=c["city"], lat=c["lat"], lng=c["lng"])
            session.add(new_city)
            session.flush()          # assign ids without committing each time
            existing[c["city"]] = new_city.id
    return existing

def upsert_risks(session, df, cities_id):
    stmt = insert(WeatherRisk).values([
        {"city_id": cities_id[r["city"]], "date": r["date"].date(),
         "temp_max": r["temperature_2m_max"], "precip_max": r["precipitation_probability_max"],
         "wind_gusts": r["wind_gusts_10m_max"], "risk_total": r["risk_score_total"],
         "temp_category": get_temp_cat(r["temperature_2m_max"]),
         "precip_category": get_precip_cat(r["precipitation_probability_max"]),
         "wind_category": get_wind_cat(r["wind_gusts_10m_max"])}
        for _, r in df.iterrows()
    ])
    stmt = stmt.on_conflict_do_update(
        constraint="unique_city_date",
        set_={
            "temp_max": stmt.excluded.temp_max,
            "precip_max": stmt.excluded.precip_max,
            "wind_gusts": stmt.excluded.wind_gusts,
            "risk_total": stmt.excluded.risk_total,
            "temp_category": stmt.excluded.temp_category,
            "precip_category": stmt.excluded.precip_category,
            "wind_category": stmt.excluded.wind_category,
        },
    )
    session.execute(stmt)
```

To use `on_conflict_do_update` add a unique constraint on `(city_id, date)` in `create_db.py`:

```python
from sqlalchemy import UniqueConstraint
__table_args__ = (UniqueConstraint("city_id", "date", name="unique_city_date"),)
```

The category functions are also pure mappings — they can be applied vectorized with `pd.cut` instead of `.apply()`, see §3.6 of `insert_silver_data`:

```python
df["temp_category"] = pd.cut(df["temperature_2m_max"], [-np.inf, 15, 25, np.inf],
                             labels=["Froid", "Modéré", "Chaud"])
df["precip_category"] = pd.cut(df["precipitation_probability_max"], [-np.inf, 20, 60, np.inf],
                               labels=["Faible", "Moyenne", "Forte"])
df["wind_category"] = pd.cut(df["wind_gusts_10m_max"], [-np.inf, 20, 50, np.inf],
                             labels=["Calme", "Modéré", "Fort"])
```

### 3.6 Secrets in source code → environment variables

**File:** `gold/create_db.py:4-9` (and mirrored in `docker-compose.yml:15-17`).

DB credentials (`ysn.mfth`) are hardcoded and committed. Move them to env vars read via `os.getenv`, and inject from Compose / the scheduler environment:

```python
import os
from sqlalchemy import create_engine

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "s_meteorisk_db")

engine = create_engine(f"postgresql+pg8000://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
```

Also consider making the host configurable — the DAG container uses `meteorisk_db` while a local dev run uses `localhost`, which is the realistic cause of connection failures between environments. Local use `localhost`, compose passes `meteorisk_db`.

### 3.7 DAG hygiene: meaningful names, modern scheduling

**File:** `dags/meteorisk_pipeline.py`.

- The callables are named `x`, `x2`, `x3` — invisible to any reader and meaningless in logs. Rename to `extract_weather`, `transform_clean_silver`, `load_gold_to_postgres`.
- `schedule_interval` is deprecated in Airflow ≥2.3 in favour of `schedule`.
- The hard-coded `sys.path.append('/opt/airflow/')` is only necessary because of the ad-hoc volume mount; if the repo is instead installed (pip config / `dags_folder` pointing at the repo), the import becomes a normal package import.

```python
default_args = {
    "owner": "yassine",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 18),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "s_meteorisk_daily_update",
    default_args=default_args,
    description="Pipeline ETL complet pour s-MeteoRisk",
    schedule="0 2 * * *",
    catchup=False,
    tags=["MeteoRisk", "ETL"],
) as dag:
    extract_task >> transform_task >> load_task
```

### 3.8 Dashboard: push filtering to the database

**File:** `dashboard/app.py:13-51`.

The whole joined table is loaded into memory and filtered client-side; when only `date`/risk filters change, nothing is re-queried thanks to `@st.cache_data`, but the full table is re-served on every cache miss. For modest data this is fine; for scale, build the `WHERE` clause from the filters and pass parameters through `pd.read_sql`:

```python
@st.cache_data(ttl=3600)
def load_data(cities, start_date, end_date, min_risk):
    query = text("""
        SELECT c.name AS city, w.date, w.temp_max, w.precip_max,
               w.wind_gusts, w.risk_total, w.temp_category,
               w.precip_category, w.wind_category
        FROM cities c
        JOIN weather_risks w ON c.id = w.city_id
        WHERE c.name = ANY(:cities)
          AND w.date BETWEEN :start_date AND :end_date
          AND w.risk_total >= :min_risk
    """)
    with engine.connect() as cnx_db:
        return pd.read_sql(query, cnx_db,
                           params={"cities": cities, "start_date": start_date,
                                   "end_date": end_date, "min_risk": min_risk})
```

(N.B. `load_data()` is currently defined *before* the sidebar filters are declared, so today it ignores the filters entirely by design — the refactor above makes the filters drive the query.)

### 3.9 Other quick wins

- **`bronze/` has three look-alike outputs** — `data.json`, `data01.json`, `raw_data.json` — two of which (`data.json`, `raw_data.json`) carry stale/legacy formats. Keep a single file name (e.g. `data.json`) with the current schema and archive/remove the others to avoid reading the wrong artifact.
- **`extrac_berto` comment cruft** — `extract_bronze.py` and `clean_silver.py` contain commented-out prints, imports and scratch lines; delete them.
- **`analysis.sql`:23** `SELECT COUNT(id) FROM cities` and the two `MAX(...)` queries can no-op or become confusing when repeated daily on empty weather tables; guard with `WHERE EXISTS` or move them into the dashboard KPIs which already compute them.
- **Consistency: `precipitation_sum` is fetched but never consumed**. Either use it (store as a column) or drop it from the API `params` to reduce the payload.