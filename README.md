# MakeMyTrip Hotel Listings — ETL Pipeline

> **Author:** Rydhi Dadigamuwa  
> **GitHub:** https://github.com/Rydhi-Dadigamuwa/Technical-Examination  
> **Dataset:** [MakeMyTrip hotel listings sample (20,046 records)](https://www.kaggle.com/datasets/PromptCloudHQ/hotels-on-makemytrip)

A production-style ETL pipeline that extracts raw hotel data, cleans and validates it, loads it into PostgreSQL, and integrates with AWS S3 for raw and processed data storage.

---

## Project Structure

```
.
├── run_pipeline.py          # Complete ETL pipeline (single file, 8 steps)
├── schema.sql               # PostgreSQL schema — tables, constraints, indexes
├── queries.sql              # 3 analytical queries with optimization notes
├── requirements.txt         # Python dependencies
├── .env.example             # Environment variable template
├── .env                     # Your actual secrets (never commit this)
├── .gitignore
├── data/
│   └── makemytrip_com-travel_sample.csv   Original dataset
└── logs/
    ├── pipeline_<timestamp>.log            ← created on each run
    └── rejected_records.csv               ← rows that failed validation
```

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone https://github.com/Rydhi-Dadigamuwa/Technical-Examination.git
cd Technical-Examination
pip install -r requirements.txt
```


### 2. Add your dataset

Place  CSV file at:
```
data/makemytrip_com-travel_sample.csv
```

### 3. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your actual AWS credentials and PostgreSQL password
```

### 4. Set up PostgreSQL

```bash
createdb hotels
psql -U postgres -d hotels -f schema.sql
```

### 5. Run the pipeline

```bash
# Full run with S3 + PostgreSQL
python run_pipeline.py

# Local only (skip S3, use SQLite)
python run_pipeline.py --skip-s3 --db-url sqlite:///data/hotels_clean.db

# Custom source and table
python run_pipeline.py --source data/myfile.csv --table my_table
```

### 6. Run analytical queries

Open pgAdmin, connect to the hotels database, and run the queries from queries.sql

---

## How It Works

When the command `python run_pipeline.py` is executed, the following process runs automatically in sequence.

First, the raw CSV file from the local machine is uploaded to AWS S3 in the `raw/` folder. This preserves the original data in the cloud before any processing begins.

Second, the pipeline downloads that same file back from S3 and loads it into memory. This confirms the S3 integration is working and ensures all data processing happens on data that has been safely stored in the cloud.

Third, the ETL process runs through all cleaning and validation stages. Finally, the cleaned dataset is uploaded back to AWS S3 in the `processed/` folder with a timestamp in the filename, creating a versioned backup of the pipeline output.

---


## Pipeline Steps

| Step | Function | Description |
|------|----------|-------------|
| 0 | `upload_raw_to_s3()` | Uploads local CSV to `s3://<bucket>/raw/` before any processing |
| 1 | `extract()` | Downloads CSV from S3 and loads into memory |
| 2 | `standardize_formats()` | Fixes dates, casing, numeric types, boolean flags |
| 3 | `clean_missing_values()` | Fills NULLs with domain-appropriate defaults |
| 4 | `remove_duplicates()` | Removes exact row duplicates and duplicate `uniq_id` rows |
| 5 | `validate_constraints()` | Applies 7 business rules — failing rows go to rejected set |
| 6 | `log_rejected_records()` | Saves rejected rows + reason to `logs/rejected_records.csv` |
| 7 | `load_to_postgres()` | Writes 13,649 clean rows to PostgreSQL |
| 8 | `upload_clean_to_s3()` | Uploads cleaned CSV to `s3://<bucket>/processed/` |


### AWS Setup Steps

1. Go to **IAM → Users → Create user**
2. Attach `AmazonS3FullAccess` policy
3. Go to **Security credentials → Create access key**
4. Copy `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your `.env`
5. Go to **S3 → Create bucket** in region `eu-north-1`
6. Add bucket name to `S3_BUCKET` in your `.env`

---

## Database Schema

### `hotel_listings` (main table)

| Column | Type | Notes |
|--------|------|-------|
| `uniq_id` | VARCHAR PRIMARY KEY | Deduplication key |
| `property_id` | VARCHAR NOT NULL | Natural business key |
| `property_name` | VARCHAR NOT NULL | Hotel name |
| `property_type` | VARCHAR | Hotel / Resort / Homestay / etc. |
| `city` | VARCHAR NOT NULL | Validated not-null |
| `state` | VARCHAR | Defaults to 'Unknown' |
| `country` | VARCHAR | Defaults to 'India' |
| `latitude` | NUMERIC | Constrained 5.0–40.0 (India bounding box) |
| `longitude` | NUMERIC | Constrained 65.0–98.0 (India bounding box) |
| `hotel_star_rating` | NUMERIC | 0–5, where 0 = unrated |
| `mmt_review_score` | NUMERIC | 0.0–5.0 |
| `mmt_review_count` | INTEGER | ≥ 0 |
| `crawl_date` | DATE | ISO format YYYY-MM-DD |

### `pipeline_audit` (audit table)

| Column | Type | Notes |
|--------|------|-------|
| `run_id` | SERIAL PRIMARY KEY | Auto increment |
| `run_at` | TIMESTAMP | When the pipeline ran |
| `source_file` | VARCHAR | Source CSV file name |
| `raw_row_count` | INTEGER | Total rows extracted |
| `clean_row_count` | INTEGER | Rows loaded to database |
| `rejected_count` | INTEGER | Rows that failed validation |
| `elapsed_seconds` | NUMERIC | Pipeline run time |
| `status` | VARCHAR | success or failed |

### Indexes

| Index | Columns | Supports |
|-------|---------|----------|
| `idx_property_type` | property_type | Query 1 — group by type |
| `idx_crawl_date` | crawl_date | Query 2 — monthly trend |
| `idx_city_state` | city, state | Query 3 — rating by city |
| `idx_star_rating` | hotel_star_rating | Dashboard star filters |
| `idx_mmt_review_score` | mmt_review_score | Score range queries |
| `idx_lat_lon` | latitude, longitude | Geo bounding-box queries |
| `idx_type_star` | property_type, hotel_star_rating | Composite dashboard filter |

---



## Optimization Decisions

### Why These Indexes Were Chosen

The goal of indexing is to avoid full sequential scans where PostgreSQL reads every single row in the table to find what it needs. With 13,649 rows this is manageable, but at 100,000 or 1 million rows a sequential scan becomes very slow. Every index in this project was chosen based on a specific query pattern.

**idx_property_type** was created because Query 1 filters and groups by property_type. Without this index PostgreSQL scans all rows to find hotels matching each type. With the index it jumps directly to the relevant rows. This is a single column B-tree index which works best for equality filters and GROUP BY operations.

**idx_crawl_date** was created because Query 2 groups by month using DATE_TRUNC on crawl_date. Date columns that are used for range queries and time-based grouping benefit greatly from B-tree indexes because dates have a natural order that the index can exploit.

**idx_city_state** is a composite index on both city and state together. This was chosen instead of two separate indexes because Query 3 groups by both columns simultaneously. A composite index covers both columns in a single index lookup which is more efficient than PostgreSQL having to merge two separate indexes.

**idx_star_rating** and **idx_mmt_review_score** were added because filtering by star rating and review score are the two most common operations in any hotel analytics dashboard. These columns appear in WHERE clauses frequently.

**idx_lat_lon** is a composite index on latitude and longitude together. Geographic queries that filter by a bounding box always use both columns together so a composite index is more efficient than two separate ones.

**idx_type_star** is a composite index on property_type and hotel_star_rating together. This covers the very common dashboard query pattern of filtering by both type and star rating at the same time such as show me all 5 star Resorts.

---

### Why Noisy Columns Were Dropped Before Loading

Seven columns including image_urls, hotel_overview, pageurl and in_your_room were dropped before loading into the database. These columns contain unstructured text and HTML content that cannot be queried or indexed meaningfully. Storing them would increase the table size significantly, slow down every query due to larger row sizes, and provide no analytical value. Dropping them reduced the column count from 33 to 26.

---

### Why Data Is Loaded in Chunks

The pipeline loads data in chunks of 1,000 rows at a time using `chunksize=1000` in the `to_sql()` call. This was chosen instead of loading all 13,649 rows in a single insert because large single inserts can lock the table for a long time and cause memory spikes. Chunked loading keeps memory usage stable and allows other database operations to proceed between chunks.

---

### Why Review Scores Were Not Filled With Defaults

When cleaning missing values, review counts were filled with 0 but review scores were intentionally left as NULL. Filling a missing review score with 0 or with an average would corrupt analytical results. A hotel with no reviews should not appear to have a score of 0 because that would make it rank below hotels that genuinely received poor reviews. Keeping it as NULL means it is correctly excluded from AVG calculations automatically by PostgreSQL.

---

### Why Validation Runs After Cleaning

The pipeline runs standardization and cleaning before validation on purpose. This order gives every row the best chance of passing validation. For example a row with a missing property_type gets filled with Unknown during cleaning, which means it does not fail the not-null check during validation. Running validation before cleaning would reject rows that could have been saved with simple defaults.

---

### How to See Index Performance

Run this in pgAdmin to see the index being used:
```sql
EXPLAIN ANALYZE
SELECT property_type, COUNT(*)
FROM hotel_listings
WHERE property_type <> 'Unknown'
GROUP BY property_type
ORDER BY 2 DESC LIMIT 10;
```

Look for **Index Scan using idx_property_type** in the output. This confirms PostgreSQL is using the index instead of a full Sequential Scan. On a table with 1 million records this difference would mean the query runs in milliseconds instead of several seconds.


## Analytical Queries

### Query 1 — Top Property Types by Review Score

Finds which hotel categories (Resort, Homestay, etc.) have the best average guest reviews and the most listings. Uses `idx_property_type` for fast aggregation.

### Query 2 — Monthly Listing Growth

Shows how many hotels were scraped per month and the cumulative total over time. Uses `idx_crawl_date` for efficient date-range grouping.

### Query 3 — Average Rating by City (Top 20)

Ranks the 20 cities with the most listings by their average review score, star rating, and location rating. Uses `idx_city_state` composite index.

---









## Scalability & Architecture (Read Word file for further)

### Scaling to 1 Million+ Records

**Chunked Processing:** Replace the current `pd.read_csv()` with `chunksize=50_000` to process large files in memory-efficient chunks rather than loading everything at once. Each chunk goes through standardization, cleaning, validation and loading independently.

**Parallel Processing:** Use Python `multiprocessing` or Apache Spark for distributed transformation when files exceed available RAM. Apache Spark can distribute processing across a cluster of machines and handle hundreds of millions of records with ease.

**Faster Database Loading:** Switch from `to_sql(method="multi")` to PostgreSQL `COPY FROM` via `psycopg2` — roughly 10 times faster for bulk inserts because it bypasses the SQL parser and writes data directly to the table.

---

### Scheduling

| Tool | Use Case |
|------|----------|
| `cron` | Simple scheduling on a single server |
| Apache Airflow | DAG-based orchestration with retries, logging, monitoring |
| AWS EventBridge + Lambda | Serverless trigger on S3 file arrival |

For production use the pipeline would be scheduled using Apache Airflow. Airflow allows you to define the pipeline as a DAG where each ETL step is a separate task. If any step fails, Airflow automatically retries it and sends an alert.

---

### Partitioning Strategy at Scale

As the hotel_listings table grows over time, the table would be partitioned by year of crawl_date using PostgreSQL range partitioning. Data from 2023 goes into one partition, 2024 into another, and so on. When a query includes a date filter, PostgreSQL only scans the relevant partition instead of the entire table. This can reduce query time by orders of magnitude on large datasets.

At 1 million records, BRIN indexes would also be added on the crawl_date column because BRIN indexes are much smaller than B-tree indexes and work extremely well on columns where data is naturally ordered over time.

---

### Failure Handling

- **Try/catch around every step** — the pipeline logs the error and exits cleanly rather than silently corrupting data
- **Rejected records audit log** — `logs/rejected_records.csv` captures every row that fails validation with a reason code
- **Idempotent loads** — `if_exists="replace"` means re-running the pipeline is safe and never creates duplicate data
- **S3 as checkpoint** — raw data is preserved in S3 before any transformation begins, so the pipeline can be restarted from the S3 file if the database load fails

---

### Cloud Native Architecture

For a fully cloud native version, the local CSV file would be replaced by an S3 event trigger. Whenever a new file lands in the S3 `raw/` folder, it would automatically trigger an AWS Lambda function or an Airflow DAG to start the pipeline. The PostgreSQL database would be replaced by Amazon RDS for managed database hosting with automatic backups, or Amazon Redshift for analytical workloads at very large scale. This architecture requires zero manual intervention — data flows from source to database completely automatically.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | For S3 | IAM access key |
| `AWS_SECRET_ACCESS_KEY` | For S3 | IAM secret key |
| `AWS_REGION` | For S3 | e.g. `eu-north-1` |
| `S3_BUCKET` | For S3 | Your S3 bucket name |
| `DB_URL` | For PostgreSQL | SQLAlchemy connection string |
| `CSV_SOURCE` | Optional | Path to raw CSV (default: `data/makemytrip_com-travel_sample.csv`) |

---

## CLI Reference

| Option | Description |
|--------|-------------|
| `--source PATH` | Local CSV/ZIP path or s3://bucket/key |
| `--db-url URL` | SQLAlchemy database URL |
| `--table NAME` | Target table name (default: hotel_listings) |
| `--s3-bucket NAME` | S3 bucket name |
| `--skip-s3` | Skip all S3 upload steps |
