# Power BI Analytics Dashboard — CRM + Sales Pipeline

End-to-end data engineering project on the Maven Analytics CRM + Sales dataset.

**Stack (all free):** Python 3.11 · pandas · PostgreSQL 16 (Docker) · Airflow · Power BI Desktop

## Status

Building in stages. See [`docs/roadmap.md`](docs/roadmap.md) for progress.

- [x] Stage 0 — Tools installed
- [x] Stage 1 — Repo scaffolded
- [ ] Stage 2 — Postgres in Docker
- [ ] Stage 3 — Extract (CSV → parquet)
- [ ] Stage 4 — Load staging
- [ ] Stage 5 — Transform → warehouse
- [ ] Stage 6 — Quality checks
- [ ] Stage 7 — Power BI dashboard
- [ ] Stage 8 — Airflow orchestration
- [ ] Stage 9 — Polish

## Data

Source: [Maven Analytics — CRM + Sales](https://mavenanalytics.io/data-playground)

Place these in `data/raw/`:
- `accounts.csv`
- `products.csv`
- `sales_pipeline.csv`
- `sales_teams.csv`

See [`docs/data_dictionary.csv`](docs/data_dictionary.csv) for column definitions.