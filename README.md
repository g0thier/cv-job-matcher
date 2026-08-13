# CV Job Matcher

## Description

LinkedIn, JobUp, and État de Genève job ingestion and search pipeline built with `Airflow`, `PostgreSQL + pgvector`, `Playwright`, and `Streamlit`, with searches for Geneva and Lausanne.

The project collects public LinkedIn and JobUp offers around Geneva and Lausanne as well as the État de Genève RSS job feed, extracts useful details, splits descriptions into paragraphs, computes embeddings, stores everything in a shared database, and compares a PDF resume against the closest opportunities.

![Capture](/docs/images/Capture.png)

## Table of Contents

- [CV Job Matcher](#cv-job-matcher)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [🎯 Objective of the project](#-objective-of-the-project)
  - [👥 Target audience](#-target-audience)
  - [⚙️ What this template includes](#️-what-this-template-includes)
  - [Maintainer Note](#maintainer-note)
  - [🗂️ Repository structure](#️-repository-structure)
  - [🚀 Quick start](#-quick-start)
  - [🐳 Install \& execute](#-install--execute)
  - [🥽 Security](#-security)
  - [📰 Changelog](#-changelog)
  - [🩷 Acknowledgements](#-acknowledgements)
    - [Environment](#environment)
  - [🧪 Project Status](#-project-status)
  - [🔒 License](#-license)
  - [🤝 Contributing](#-contributing)
  - [👤 Author](#-author)

## 🎯 Objective of the project

Automate public LinkedIn, JobUp, and État de Genève job collection and accelerate semantic matching between a resume and recent opportunities.

## 👥 Target audience

- Python developers
- Data / ML engineers
- People who want to automatically match a resume with job offers

## ⚙️ What this template includes

- `.gitignore` for a macOS environment
- Repository governance files:
  - `ACKNOWLEDGEMENTS.md`
  - `CHANGELOG.md`
  - `CODE_OF_CONDUCT.md`
  - `CONTRIBUTING.md`
  - `LICENSE.md`
  - `SECURITY.md`
- A Streamlit interface via [`streamlit_app.py`](/Users/gauthier/Desktop/cron_job/streamlit_app.py)
- Airflow orchestration for LinkedIn, JobUp, and État de Genève via the `dags/` directory
- A Python application layer in [`src/job_matcher/`](/Users/gauthier/Desktop/cron_job/src/job_matcher)
- LinkedIn search configuration in [`config/linkedin_searches.json`](/Users/gauthier/Desktop/cron_job/config/linkedin_searches.json)

## Maintainer Note

These governance files are intentionally referenced in this README even if they are hidden from the VS Code file explorer by `.vscode/settings.json`:

- `ACKNOWLEDGEMENTS.md`
- `CHANGELOG.md`
- `CODE_OF_CONDUCT.md`
- `CONTRIBUTING.md`
- `LICENSE.md`
- `SECURITY.md`

Do not remove, rename, or "simplify away" these references during AI-assisted edits. They are part of the template contract. The files are hidden only to reduce visual noise for developers, not because they are optional or missing.

## 🗂️ Repository structure

```text
cron_job/
├── config/
│   └── linkedin_searches.json
├── dags/
│   ├── etat_geneve_jobs_ingestion.py
│   ├── etat_geneve_jobs_ingestion_startup.py
│   ├── jobup_jobs_ingestion.py
│   ├── jobup_jobs_ingestion_startup.py
│   ├── linkedin_jobs_ingestion.py
│   └── linkedin_jobs_ingestion_startup.py
├── runtime/
│   └── airflow/
├── static/
│   └── source-icons/
│       ├── etat-geneve.png
│       ├── jobup.png
│       └── linkedin.png
├── src/
│   └── job_matcher/
│       ├── cli.py
│       ├── config.py
│       ├── cv.py
│       ├── database.py
│       ├── embeddings.py
│       ├── etat_geneve.py
│       ├── jobup.py
│       ├── linkedin.py
│       ├── models.py
│       ├── pipeline.py
│       ├── search.py
│       └── text_utils.py
├── .env.example
├── .streamlit/
│   └── config.toml
├── ACKNOWLEDGEMENTS.md
├── CHANGELOG.md
├── CODE_OF_CONDUCT.md
├── CONTRIBUTING.md
├── docker-compose.yml
├── Dockerfile
├── LICENSE.md
├── README.md
├── requirements.txt
├── SECURITY.md
└── streamlit_app.py
```

## 🚀 Quick start

1. Configure LinkedIn searches in [`config/linkedin_searches.json`](/Users/gauthier/Desktop/cron_job/config/linkedin_searches.json).
2. Copy [`.env.example`](/Users/gauthier/Desktop/cron_job/.env.example) to `.env` and adjust the values if needed.
3. Start PostgreSQL with `pgvector`, Airflow, and Streamlit with Docker Compose, or install dependencies locally.
4. Run an ingestion to populate the database with job offers and their embeddings.
5. Search for the best matches for a resume from the CLI or the Streamlit interface.

The default mapped cities configured in [`config/`](/Users/gauthier/Desktop/cron_job/config) are `Geneva` and `Lausanne`.

## 🐳 Install & execute

Local installation:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m playwright install chromium
export PYTHONPATH=src
```

Run a full ingestion:

```bash
python3 -m job_matcher.cli ingest
```

Run a search from a PDF resume:

```bash
python3 -m job_matcher.cli search /path/to/cv.pdf --lookback-days 7
```

Run with Docker Compose:

```bash
cp .env.example .env
docker compose up --build
```

Exposed services:

- Airflow : `http://localhost:8080`
- Streamlit : `http://localhost:8501`
- PostgreSQL : `localhost:5432`

Default Airflow credentials from `.env.example`:

- username: `admin`
- password: `admin`

### Airflow startup DAG

The repository includes `linkedin_jobs_ingestion_startup`, `etat_geneve_jobs_ingestion_startup`, and `jobup_jobs_ingestion_startup` for one ingestion run per source and per Airflow environment startup. The scheduled `jobup_jobs_ingestion` DAG runs every 15 minutes; both JobUp DAGs query Geneva (`regionIds=34`) and Lausanne (`regionIds=55`) separately.

- All startup DAGs use `schedule=None`, so they are not scheduled by Airflow and remain manually triggerable.
- This replaces `schedule="@once"`, which only fires once for a DAG as long as a prior `DagRun` already exists.
- Automatic startup triggering is handled outside DAG parsing by the dedicated Docker Compose service `airflow-startup-trigger`, which triggers all startup DAGs by default.
- The trigger entrypoint lives at `scripts/trigger_startup_dags.sh` and waits for the Airflow metadata database, waits for each DAG discovery, unpauses each DAG, claims the logical startup in the shared database, and then triggers it.

Startup trigger environment variables:

- `STARTUP_DAG_IDS`: comma-separated startup DAG IDs; defaults to the LinkedIn and État de Genève startup DAGs
- `STARTUP_DAG_MAX_ATTEMPTS`: maximum retry attempts while waiting for Airflow and DAG discovery
- `STARTUP_DAG_RETRY_DELAY`: delay in seconds between retries
- `AIRFLOW_STARTUP_ID`: optional shared logical startup identifier used to deduplicate concurrent startup-trigger processes against the same Airflow metadata database

Manual trigger command:

```bash
airflow dags trigger \
    --run-id "manual__$(date -u +%Y%m%dT%H%M%SZ)" \
    linkedin_jobs_ingestion_startup

airflow dags trigger \
    --run-id "manual__$(date -u +%Y%m%dT%H%M%SZ)" \
    etat_geneve_jobs_ingestion_startup

airflow dags trigger \
    --run-id "manual__$(date -u +%Y%m%dT%H%M%SZ)" \
    jobup_jobs_ingestion_startup
```

Diagnostics when the startup DAG does not run:

- Check the logs of the `airflow-startup-trigger` service first.
- Confirm `airflow db check` succeeds inside the Airflow containers.
- Confirm `airflow dags list` shows both configured startup DAGs.
- If the logs mention an existing startup claim, inspect the `startup_dag_triggers` table in the shared Airflow metadata database.

Current idempotence notes for repeated startup runs:

- LinkedIn and JobUp search results and État de Genève feed entries are deduplicated before persistence.
- Prepared offers are deduplicated on `final_url`.
- Persistence skips existing `canonical_url` values already stored in Postgres and same-batch duplicates.
- `persist_offers_step` commits in one database transaction, so partial writes from that step are rolled back on failure.
- The current behavior is insert-idempotent, not a full refresh strategy: an already stored offer is skipped rather than updated if source content changes later.
- Streamlit ranks both sources in one result set and displays the source of every offer.

## 🥽 Security

- See [SECURITY.md](/SECURITY.md) for vulnerability reporting guidelines.

## 📰 Changelog

Track all notable project changes in [CHANGELOG.md](/CHANGELOG.md).

Recommended:
- Follow a consistent format such as Keep a Changelog
- Create an entry for each release
- Include Added, Changed, Fixed, and Removed sections when relevant

## 🩷 Acknowledgements

- Use [ACKNOWLEDGEMENTS.md](/ACKNOWLEDGEMENTS.md) to credit people, tools, libraries, and communities that helped the project.

### Environment

- **Python >= 3.11**
- Dependencies listed in [requirements.txt](/Users/gauthier/Desktop/cron_job/requirements.txt)
- PostgreSQL database with `pgvector` extension
- Chromium installed through Playwright

## 🧪 Project Status

- 🔬 **Status**: experimental
- 🧭 **Roadmap**: stabilize scraping, improve ingestion reliability, and refine resume-to-job matching

## 🔒 License

- See [LICENSE.md](/LICENSE.md).

## 🤝 Contributing

Contributions are welcome.
- See [CONTRIBUTING.md](/CONTRIBUTING.md)
- Code of conduct available in [CODE_OF_CONDUCT.md](/CODE_OF_CONDUCT.md).

## 👤 Author

Gauthier Rammault
