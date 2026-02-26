# Spark-Based Data Ingestion Pipeline
Author: gardhaniissaac

Stack:
- Apache Airflow
- Apache Spark (local mode)
- PostgreSQL
- Docker & Docker Compose

## Project Structure
Project Structure
```.
├── Dockerfile
├── README.md
├── config
│   └── airflow.cfg
├── dags
│   ├── configs
│   │   ├── assessments.yaml
│   │   ├── assessments_raw.yaml
│   │   ├── attendances.yaml
│   │   ├── attendances_raw.yaml
│   │   ├── daily_performances.yaml
│   │   ├── students.yaml
│   │   └── students_raw.yaml
│   ├── resources
│   │   ├── assessments.json
│   │   ├── attendances.csv
│   │   └── students.csv
│   ├── scripts
│   │   ├── tests
│   │   │   └── schema_loader_test.py
│   │   ├── db.py
│   │   ├── schema_loader.py
│   │   └── spark_ingestion.py
│   ├── assessments_dag.py
│   ├── assessments_raw_dag.py
│   ├── attendances_dag.py
│   ├── attendances_raw_dag.py
│   ├── daily_performance_dag.py
│   ├── students_dag.py
│   └── students_raw_dag.py
├── docker-compose.yaml
└── requirements.txt
```
---

# 🚀 First Time Setup

## 1. Clone Repository

```bash
git clone <your-repo-url>
cd <your-repo-folder>
```

---

## 2. Create `.env` File

Create a file named `.env` in the project root:

```bash
touch .env
```

Generate secret key
```
openssl rand -hex 32
```

Add the following variables:

```env
# Airflow
AIRFLOW_UID=1000
AIRFLOW__WEBSERVER__SECRET_KEY=<generated-secret-key>
AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# Postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
```

Save the file.

---

## 3. Build Docker Images

```bash
docker compose build --no-cache
```

---

## 4. Initialize Airflow (Run Once)

```bash
docker compose up airflow-init
```

Wait until it finishes successfully.

---

## 5. Start All Services

```bash
docker compose up
```

---

# 🌐 Access Airflow

Open in browser:

```
http://localhost:8080
```

Login using:

```
Username: admin
Password: admin
```

Enable and trigger the DAG from the UI.

---

# 🛑 Stop Services

```bash
docker compose down
```

To completely reset (including database):

```bash
docker compose down -v
```

---

# 🔄 Rebuild After Dependency Changes

If you modify:
- Dockerfile
- requirements.txt

Run:

```bash
docker compose down
docker compose build --no-cache
docker compose up
```