# bill-dbt-migration
dbt migration from Snowflake to Trino/Iceberg — BILL financial domain
# BILL Financial Ops — dbt Migration: Snowflake → Trino/Iceberg

> Demonstrating production-grade dbt model migration from Snowflake SQL 
> to Trino SQL — the exact technical challenge faced by BILL's Data 
> Operations team.

![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-connected-29B5E8)
![Trino](https://img.shields.io/badge/Trino-435-DD00A1)
![Tests](https://img.shields.io/badge/tests-16%20passing-brightgreen)
![Models](https://img.shields.io/badge/models-10%20migrated-blue)

---

## What This Project Demonstrates

This project mirrors BILL's real-world data engineering challenge:
migrating a complete dbt analytics pipeline across SQL engines while
maintaining data quality, output parity, and best practices.

| Skill | How It Is Demonstrated |
|---|---|
| dbt modeling | Bronze / silver / gold medallion architecture |
| SQL dialect migration | Snowflake → Trino with documented changes |
| Data quality | 16 schema tests across all staging models |
| Apache Iceberg | Trino memory connector simulating Iceberg behavior |
| Git workflow | Meaningful commit history, secured credentials |
| Financial domain | Invoices, payments, AP aging, vendor spend |

---

## Business Context

BILL is a financial operations platform processing billions of dollars 
in B2B payments annually. Their core data entities are:

- **Invoices** — bills submitted by vendors for payment
- **Payments** — ACH, card, wire, and international transactions
- **Vendors** — companies receiving payments
- **Approvals** — internal approval workflow for high-value invoices

This project models all four entities across a medallion architecture,
then migrates the complete dbt layer from Snowflake to Trino.

---

## Architecture!
[Architecture](docs/architecture.png)

The pipeline follows a medallion architecture across two SQL engines:

- **Bronze** — raw data ingested and type-cast from CSV seeds
- **Silver** — business logic joins across invoices, payments, and vendors  
- **Gold** — aggregated mart tables ready for reporting
- **Migration** — every model translated from Snowflake to Trino SQL dialect

---

## Repository Structure
bill-dbt-migration/
├── data/
│   ├── generate_data.py        ← Synthetic data generator
│   └── seed/                   ← 500 invoices, 227 payments, 50 vendors
├── snowflake_project/
│   ├── models/
│   │   ├── staging/            ← 4 bronze models
│   │   ├── intermediate/       ← 3 silver models
│   │   └── marts/              ← 3 gold tables
│   └── seeds/                  ← CSV files loaded to Snowflake
├── trino_project/
│   └── models/                 ← Same 10 models in Trino SQL dialect
├── trino/
│   └── docker-compose.yml      ← Local Trino engine
└── docs/
├── architecture.png        ← Pipeline diagram
└── sql_dialect_cheatsheet.md
---

## Key SQL Dialect Differences Found

Full details in [docs/sql_dialect_cheatsheet.md](docs/sql_dialect_cheatsheet.md)

| Snowflake | Trino | Impact |
|---|---|---|
| `DATEDIFF('day', a, b)` | `date_diff('day', a, b)` | All date calculations |
| `CURRENT_TIMESTAMP()` | `current_timestamp` | All models |
| `CURRENT_DATE()` | `current_date` | Mart aging buckets |
| `ZEROIFNULL(x)` | `coalesce(x, 0)` | Payment variance calc |
| `LEFT JOIN ... USING` | `LEFT JOIN ... ON` | All intermediate joins |

---

## How to Run

### Snowflake Project

```bash
cd snowflake_project
dbt seed --profiles-dir .
dbt build --profiles-dir .
```

### Trino Project

```bash
cd trino
docker-compose up -d
cd ../trino_project
dbt run --profiles-dir .
```

---

## Data Quality

16 schema tests across all staging models — all passing.

```bash
dbt test --profiles-dir .
```

Tests cover uniqueness, not null constraints, and accepted value 
validation across all four staging models.

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.12 | Data generation |
| dbt Core | 1.11 | Model development |
| Snowflake | Free tier | Source warehouse |
| Trino | 435 | Target query engine |
| Docker | 29.4 | Local Trino runtime |
| GitHub | — | Version control |