# s-MeteoRisk — Quick Start Guide

This guide walks you through the ETL pipeline from zero **Start** through **Verify** to **Stop**, using only Docker commands.

Prerequisites before you begin:

- Docker and Docker Compose are installed and running.
- You are in the project root folder (`s-MeteoRisk`) for every command below.

---

## 1. Start the project

### 1.1 Confirm Docker is ready

```bash
docker --version
docker compose version
```

These print the installed Docker and Compose versions, confirming your environment is ready before anything else runs.

### 1.2 Build and start every service

```bash
docker compose up -d
```

This builds (if needed) the custom `pipeline` image, pulls the required public images (`postgres:16`, `apache/airflow:2.7.1`), creates the containers described in `docker-compose.yml`, and starts the Database, Airflow scheduler, and Airflow webserver in the background.

### 1.3 Watch the services start live (optional)

```bash
docker compose logs -f
```

This streams the real-time console output of all containers to your terminal so you can watch Airflow and PostgreSQL boot up; press `Ctrl+C` to stop following the logs without stopping the containers.

---

## 2. Verify the project

### 2.1 Check container status

```bash
docker compose ps
```

This lists every running container with its current state (Up / Exited / Restarting) and the ports it exposes, so you can confirm nothing crashed at boot.

### 2.2 Open the Airflow UI

```bash
start http://localhost:8080
```

This opens the Airflow webserver in your browser; log in with `airflow` / `airflow`, then toggle the **s_meteorisk_daily_update** DAG and trigger a run.

### 2.3 Trigger the DAG from the command line (optional)

```bash
docker compose exec airflow-scheduler airflow dags trigger s_meteorisk_daily_update
```

This executes the command inside the running scheduler container, asking Airflow to launch the `s_meteorisk_daily_update` DAG immediately instead of waiting for the daily 02:00 schedule.

### 2.4 Check the PostgreSQL database

```bash
docker compose exec db psql -U postgres -d s_meteorisk_db -c "\dt"
```

This opens a short-lived command line session inside the `db` container, connects as the `postgres` user to the `s_meteorisk_db` database, and lists its tables (`cities`, `weather_risks`) to confirm the schema exists.

### 2.5 See the created tables (optional)

```bash
docker compose exec db psql -U postgres -d s_meteorisk_db -c "SELECT COUNT(*) FROM weather_risks;"
```

This runs a SQL query inside the database container and prints how many risk records were inserted, confirming the ETL actually loaded data.

---

## 3. Run the Streamlit dashboard (optional)

```bash
streamlit run dashboard/app.py
```

This launches the Streamlit dashboard on your host machine (default `http://localhost:8501`) so you can browse the KPIs and charts that read from the database.

> **Note:** the dashboard currently expects the host name `meteorisk_db`. If it cannot reach the database from your host, start it with:
> `docker compose exec pipeline python -m streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0`

---

## 4. Stop the project

### 4.1 Stop all containers (keep the data)

```bash
docker compose down
```

This gracefully stops and removes the containers, but keeps the `pg_data` volume so all database content survives for your next run.

### 4.2 Stop everything and erase the data (full reset)

```bash
docker compose down -v
```

This does the same as `docker compose down` but also deletes the named `pg_data` volume, wiping all stored data for a clean slate.

### 4.3 Remove unused images (optional cleanup)

```bash
docker system prune
```

This deletes stopped containers, unused networks, and dangling images no longer referenced, freeing disk space without touching your running services.