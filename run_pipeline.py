import os
import re
import sys
import logging
import argparse
import zipfile
import io
import sqlite3
import warnings
from datetime import datetime
import pandas as pd
import numpy as np

# suppress warnings, create output dirs, and configure logging to file and console
warnings.filterwarnings("ignore")

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)
LOG_FILE = f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# CONSTANTS

# Raw property type strings → canonical Title Case; unmapped → 'Unknown'
PROPERTY_TYPE_MAP = {
    "hotel": "Hotel", "homestay": "Homestay", "lodge": "Lodge",
    "houseboat": "Houseboat", "apartment": "Apartment", "palace": "Palace",
    "guest house": "Guest House", "guesthouse": "Guest House",
    "camp": "Camp", "cottage": "Cottage", "resort": "Resort",
    "villa": "Villa", "hostel": "Hostel", "motel": "Motel", "inn": "Inn",
}

# Textual/numeric star rating strings → float 1.0–5.0; "0" → None (unrated)
STAR_RATING_MAP = {
    "1 star": 1.0, "2 star": 2.0, "3 star": 3.0, "4 star": 4.0, "5 star": 5.0,
    "four star": 4.0, "five star": 5.0,
    "three on 5": 3.0, "four on 5": 4.0, "five on 5": 5.0,
    "1": 1.0, "2": 2.0, "3": 3.0, "4": 4.0, "5": 5.0,
    "0": None,
}

TEMPLATE_RE = re.compile(r"\{\{.*?\}\}")  # Detects unfilled {{placeholders}} → null
DATE_FMTS   = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y"]  # Tried in order

# India geographic bounding box — rows outside these ranges are rejected
LAT_MIN, LAT_MAX = 5.0,  40.0
LON_MIN, LON_MAX = 65.0, 98.0

# Noisy/unprocessable columns dropped before loading to the database
DROP_COLS = [
    "highlight_value", "mmt_traveller_type_review_count",
    "image_urls", "hotel_overview", "in_your_room", "pageurl", "qts",
]


# STEP 0 — UPLOAD RAW FILE TO S3

def upload_raw_to_s3(local_path: str, bucket: str) -> str:
    """Upload local CSV to s3://<bucket>/raw/. Returns the S3 URI, or None if skipped/failed."""
    if not bucket:
        logger.info("[S3 UPLOAD] No S3_BUCKET set — skipping raw upload")
        return None

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        logger.warning("[S3 UPLOAD] boto3 not installed — skipping. Run: pip install boto3")
        return None

    if not os.path.exists(local_path):
        logger.error(f"[S3 UPLOAD] Local file not found: {local_path}")
        return None

    filename  = os.path.basename(local_path)
    s3_key    = f"raw/{filename}"
    s3_uri    = f"s3://{bucket}/{s3_key}"

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "ap-south-1"),
        )
        logger.info(f"[S3 UPLOAD] Uploading '{local_path}' → {s3_uri} ...")
        s3.upload_file(local_path, bucket, s3_key)
        logger.info(f"[S3 UPLOAD] ✓ Raw file uploaded to {s3_uri}")
        return s3_uri

    except (BotoCoreError, ClientError) as e:
        logger.error(f"[S3 UPLOAD] Upload failed: {e}")
        logger.info("[S3 UPLOAD] Continuing pipeline with local file ...")
        return None


# STEP 1 — EXTRACT

def extract(source: str) -> pd.DataFrame:
    """Load raw data from a local CSV, ZIP, or S3 URI into a DataFrame (all columns as strings)."""
    logger.info(f"[EXTRACT] Source: {source}")

    if source and source.startswith("s3://"):
        df = _extract_s3(source)
    elif source and source.endswith(".zip"):
        df = _extract_zip(source)
    else:
        df = _extract_csv(source)

    logger.info(f"[EXTRACT] Loaded {len(df):,} rows x {len(df.columns)} columns")
    return df


def _extract_csv(path: str) -> pd.DataFrame:
    """Read a local CSV file into a DataFrame."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"File not found: '{path}'\n"
            f"Place makemytrip_com-travel_sample.csv at that path and retry."
        )
    return pd.read_csv(
        path, dtype=str, keep_default_na=False,
        na_values=["", "NA", "NULL", "None", "nan", "NaN", "N/A"],
        on_bad_lines="skip", encoding="utf-8", encoding_errors="replace",
    )


def _extract_zip(path: str) -> pd.DataFrame:
    """Extract the first CSV found inside a ZIP archive and return it as a DataFrame."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"ZIP not found: '{path}'")
    with zipfile.ZipFile(path) as z:
        csv_files = [n for n in z.namelist() if n.endswith(".csv")]
        if not csv_files:
            raise ValueError(f"No CSV file found inside {path}")
        logger.info(f"[EXTRACT] Found '{csv_files[0]}' inside ZIP")
        with z.open(csv_files[0]) as f:
            return pd.read_csv(
                f, dtype=str, keep_default_na=False,
                na_values=["", "NA", "NULL", "None", "nan", "NaN", "N/A"],
                on_bad_lines="skip", encoding_errors="replace",
            )


def _extract_s3(s3_uri: str) -> pd.DataFrame:
    """Download a CSV directly from S3 and return it as a DataFrame."""
    try:
        import boto3
    except ImportError:
        raise ImportError("Run: pip install boto3")

    bucket, key = s3_uri[5:].split("/", 1)
    s3   = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-south-1"),
    )
    logger.info(f"[EXTRACT] Downloading from S3: {s3_uri}")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_csv(
        io.BytesIO(body), dtype=str, keep_default_na=False,
        na_values=["", "NA", "NULL", "None", "nan", "NaN", "N/A"],
        on_bad_lines="skip",
    )



# STEP 2 — STANDARDIZE FORMATS

def _parse_star_rating(val):
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    if s in STAR_RATING_MAP:
        return STAR_RATING_MAP[s]
    m = re.match(r"^(\d+(?:\.\d+)?)", s)
    if m:
        v = float(m.group(1))
        return v if 1.0 <= v <= 5.0 else None
    return None


def _parse_score(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if ":" in s:
        s = s.split(":")[-1]
    s = s.replace("/5", "").strip()
    if not s:
        return None
    try:
        v = float(s)
        return round(v, 1) if 0.0 <= v <= 5.0 else None
    except ValueError:
        return None


def _parse_date(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(str(val).strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_timestamp(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        ts = pd.to_datetime(str(val), utc=True, errors="coerce")
        return None if pd.isna(ts) else ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _parse_review_count(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if "/" in s or not s:
        return None
    try:
        return int(float(s.replace(",", "")))
    except ValueError:
        return None


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize all column formats:
    - Strip whitespace from all string columns
    - hotel_star_rating → float 1.0–5.0
    - Review scores → float 0.0–5.0
    - Review counts → integer
    - crawl_date → YYYY-MM-DD
    - query_time_stamp → UTC datetime string
    - latitude / longitude → float
    - property_type → canonical Title Case via mapping
    - country, city, state, property_name → Title Case
    - is_value_plus → 0/1 integer
    """
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

    df["hotel_star_rating"] = df["hotel_star_rating"].apply(_parse_star_rating)
    logger.info(f"[STANDARDIZE] hotel_star_rating → float  "
                f"({df['hotel_star_rating'].isna().sum():,} invalid → NaN)")

    tmpl_mask = df["mmt_review_rating"].apply(
        lambda v: bool(TEMPLATE_RE.search(str(v))) if not pd.isna(v) else False
    )
    df.loc[tmpl_mask, "mmt_review_rating"] = None
    logger.info(f"[STANDARDIZE] mmt_review_rating: nulled {tmpl_mask.sum():,} template values")

    df["mmt_review_score"]    = df["mmt_review_score"].apply(_parse_score)
    df["site_review_rating"]  = df["site_review_rating"].apply(_parse_score)
    df["mmt_location_rating"] = df["mmt_location_rating"].apply(_parse_score)
    logger.info("[STANDARDIZE] Review scores → float 0.0–5.0")

    for col in ["mmt_review_count", "mmt_holidayiq_review_count",
                "mmt_tripadvisor_count", "site_review_count"]:
        if col in df.columns:
            df[col] = df[col].apply(_parse_review_count)
    logger.info("[STANDARDIZE] Review counts → integer")

    df["crawl_date"] = df["crawl_date"].apply(_parse_date)
    logger.info(f"[STANDARDIZE] crawl_date → YYYY-MM-DD  "
                f"({df['crawl_date'].isna().sum():,} unparseable → NaN)")

    df["query_time_stamp"] = df["query_time_stamp"].apply(_parse_timestamp)
    logger.info("[STANDARDIZE] query_time_stamp → UTC datetime string")

    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce").round(6)
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").round(6)
    logger.info("[STANDARDIZE] latitude / longitude → float")

    df["property_type"] = df["property_type"].str.lower().str.strip().map(PROPERTY_TYPE_MAP)
    logger.info(f"[STANDARDIZE] property_type → canonical  "
                f"({df['property_type'].isna().sum():,} unmapped → NaN)")

    for col in ["country", "city", "state", "property_name", "property_address"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()
    logger.info("[STANDARDIZE] country / city / state / property_name → Title Case")

    df["is_value_plus"] = (
        df["is_value_plus"].str.lower().str.strip()
        .map({"yes": 1, "no": 0, "true": 1, "false": 0, "1": 1, "0": 0})
    )
    logger.info("[STANDARDIZE] is_value_plus → 0/1 integer")

    return df



# STEP 3 — CLEAN MISSING VALUES

def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill NULL values with sensible defaults:
    - country/state/area/property_type → 'Unknown' or 'India'
    - hotel_star_rating → 0.0 (unrated)
    - is_value_plus → 0
    - Review counts → 0 (no reviews recorded)
    - Review scores intentionally kept as NaN (no fabricated values)
    """
    before = df.isnull().sum().sum()

    df["country"]           = df["country"].fillna("India")
    df["state"]             = df["state"].fillna("Unknown")
    df["area"]              = df["area"].fillna("Unknown")
    df["property_type"]     = df["property_type"].fillna("Unknown")
    df["hotel_star_rating"] = df["hotel_star_rating"].fillna(0.0)
    df["is_value_plus"]     = df["is_value_plus"].fillna(0)

    for col in ["mmt_review_count", "mmt_holidayiq_review_count", "mmt_tripadvisor_count"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    after = df.isnull().sum().sum()
    logger.info(f"[CLEAN]    Filled {before - after:,} null values  "
                f"({after:,} remain as intentional NaN)")
    return df



# STEP 4 — REMOVE DUPLICATES

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows in two passes:
    1. Exact row duplicates (all columns identical)
    2. Duplicate uniq_id values (same hotel scraped multiple times — keep first)
    """
    before = len(df)
    df = df.drop_duplicates()
    exact = before - len(df)

    before2 = len(df)
    df = df.drop_duplicates(subset=["uniq_id"], keep="first")
    uid_dupe = before2 - len(df)

    logger.info(f"[DEDUPE]   Removed {exact:,} exact duplicate rows")
    logger.info(f"[DEDUPE]   Removed {uid_dupe:,} duplicate uniq_id rows")
    logger.info(f"[DEDUPE]   Rows remaining: {len(df):,}")
    return df


# STEP 5 — VALIDATE CONSTRAINTS

def validate_constraints(df: pd.DataFrame) -> tuple:
    """
    Business validation rules:
        V1  property_name      must not be null
        V2  property_id        must not be null  (natural primary key)
        V3  uniq_id            must not be null  (deduplication key)
        V4  city               must not be null
        V5  latitude           5.0–40.0          (India geographic bounding box)
        V6  longitude          65.0–98.0         (India geographic bounding box)
        V7  hotel_star_rating  0–5 when not null

    Returns (clean_df, rejected_df). rejected_df has a 'rejection_reason' column.
    """
    rules = {
        "V1_null_property_name":  df["property_name"].isna(),
        "V2_null_property_id":    df["property_id"].isna(),
        "V3_null_uniq_id":        df["uniq_id"].isna(),
        "V4_null_city":           df["city"].isna(),
        "V5_invalid_latitude":    (df["latitude"].isna() |
                                   (df["latitude"] < LAT_MIN) |
                                   (df["latitude"] > LAT_MAX)),
        "V6_invalid_longitude":   (df["longitude"].isna() |
                                   (df["longitude"] < LON_MIN) |
                                   (df["longitude"] > LON_MAX)),
        "V7_invalid_star_rating": (df["hotel_star_rating"].notna() &
                                   (df["hotel_star_rating"] > 5)),
    }

    any_bad = pd.Series(False, index=df.index)
    for mask in rules.values():
        any_bad |= mask

    rejected_df = df[any_bad].copy()

    def _first_failure(row):
        for rule_name, mask in rules.items():
            if mask.loc[row.name]:
                return rule_name
        return "unknown"

    rejected_df["rejection_reason"] = rejected_df.apply(_first_failure, axis=1)
    clean_df = df[~any_bad].copy()

    logger.info("[VALIDATE] Constraint check results:")
    for rule_name, mask in rules.items():
        n = int(mask.sum())
        if n:
            logger.info(f"  {rule_name:<38} {n:>6,} rows failed")

    logger.info(f"[VALIDATE] Clean: {len(clean_df):,}    Rejected: {len(rejected_df):,}")
    return clean_df, rejected_df



# STEP 6 — LOG REJECTED RECORDS

def log_rejected_records(rejected_df: pd.DataFrame) -> None:
    """
    Persist all rejected rows with their rejection reason to a CSV audit file.
    This enables downstream teams to inspect and fix data quality issues.
    """
    path = "logs/rejected_records.csv"
    rejected_df.to_csv(path, index=False)
    logger.info(f"[REJECTED] {len(rejected_df):,} rows saved → {path}")
    if len(rejected_df) > 0:
        logger.info("[REJECTED] Breakdown by rejection reason:")
        for reason, count in rejected_df["rejection_reason"].value_counts().items():
            logger.info(f"  {reason:<42} {count:>6,} rows")


# STEP 7 — LOAD TO POSTGRESQL (or SQLite fallback)

def load_to_postgres(df: pd.DataFrame, db_url: str, table: str) -> None:
    """
    Load clean DataFrame into PostgreSQL (primary) or SQLite (fallback).
    Drops noisy/unprocessable columns before loading.
    Verifies row count after load.
    """
    drop_existing = [c for c in DROP_COLS if c in df.columns]
    df = df.drop(columns=drop_existing)
    logger.info(f"[LOAD]     Dropped {len(drop_existing)} noisy columns")
    logger.info(f"[LOAD]     Final shape: {df.shape[0]:,} rows x {df.shape[1]} columns")

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(db_url, echo=False)
        logger.info(f"[LOAD]     Writing to '{table}' via SQLAlchemy ...")

        df.to_sql(table, engine, if_exists="replace",
                  index=False, chunksize=1000, method="multi")

        with engine.connect() as conn:
            count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()

        status = "✓" if count == len(df) else "⚠ mismatch"
        logger.info(f"[LOAD]     {status} Verified {count:,} rows in '{table}'")
        return

    except ImportError:
        pass

    if db_url.startswith("sqlite:///"):
        db_path = db_url.replace("sqlite:///", "")
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
        logger.info(f"[LOAD]     Writing to SQLite (stdlib): {db_path}")

        conn = sqlite3.connect(db_path)
        df.to_sql(table, conn, if_exists="replace", index=False, chunksize=1000)
        count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        conn.commit()
        conn.close()

        status = "✓" if count == len(df) else "⚠ mismatch"
        logger.info(f"[LOAD]     {status} Verified {count:,} rows in '{table}'")
    else:
        raise ImportError(
            "SQLAlchemy is required for PostgreSQL.\n"
            "Run: pip install sqlalchemy psycopg2-binary"
        )


# STEP 8 — UPLOAD CLEAN DATA TO S3

def upload_clean_to_s3(df: pd.DataFrame, bucket: str) -> None:
    """
    Export clean DataFrame as CSV and upload to S3 processed zone.
    This creates a versioned backup of every pipeline run's output.
    """
    if not bucket:
        logger.info("[S3 CLEAN] No S3_BUCKET set — skipping processed upload")
        return

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        logger.warning("[S3 CLEAN] boto3 not installed — skipping upload")
        return

    date_str  = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key    = f"processed/hotels_clean_{date_str}.csv"
    s3_uri    = f"s3://{bucket}/{s3_key}"

    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes  = csv_buffer.getvalue().encode("utf-8")

        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "ap-south-1"),
        )
        s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_bytes)
        logger.info(f"[S3 CLEAN] ✓ Clean data uploaded → {s3_uri}")

    except (BotoCoreError, ClientError) as e:
        logger.error(f"[S3 CLEAN] Upload failed: {e}")


# ARGUMENT PARSING + MAIN ORCHESTRATOR

def parse_args():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    p = argparse.ArgumentParser(description="MakeMyTrip Hotel ETL Pipeline")
    p.add_argument(
        "--source",
        default=os.getenv("CSV_SOURCE", "data/makemytrip_com-travel_sample.csv"),
        help="Local CSV/ZIP path or s3://bucket/key",
    )
    p.add_argument(
        "--db-url",
        default=os.getenv("DB_URL", "sqlite:///data/hotels_clean.db"),
        help=(
            "SQLAlchemy DB URL.\n"
            "  SQLite (default): sqlite:///data/hotels_clean.db\n"
            "  PostgreSQL      : postgresql://user:pass@localhost:5432/hotels"
        ),
    )
    p.add_argument(
        "--table",
        default="hotel_listings",
        help="Target table name (default: hotel_listings)",
    )
    p.add_argument(
        "--s3-bucket",
        default=os.getenv("S3_BUCKET", ""),
        help="S3 bucket name for raw + processed uploads (optional)",
    )
    p.add_argument(
        "--skip-s3",
        action="store_true",
        help="Skip all S3 upload steps (run locally only)",
    )
    return p.parse_args()


def main():
    args  = parse_args()
    start = datetime.now()
    bucket = "" if args.skip_s3 else args.s3_bucket

    logger.info("=" * 65)
    logger.info("  MAKEMYTRIP HOTEL LISTINGS — ETL PIPELINE")
    logger.info("=" * 65)
    logger.info(f"  Source   : {args.source}")
    logger.info(f"  Database : {args.db_url}")
    logger.info(f"  Table    : {args.table}")
    logger.info(f"  S3 Bucket: {bucket or '(not configured)'}")
    logger.info(f"  Log file : {LOG_FILE}")
    logger.info("=" * 65)

    # Step 0 — Upload raw CSV to S3 (landing zone)
    logger.info("\n STEP 0: UPLOAD RAW FILE TO S3")
    if not args.source.startswith("s3://"):
        s3_raw_uri = upload_raw_to_s3(args.source, bucket)
        # If upload succeeded and we want to extract from S3, use the URI
        # Otherwise fall through to local extraction below
        extract_source = s3_raw_uri if s3_raw_uri else args.source
    else:
        extract_source = args.source
        logger.info("[S3 UPLOAD] Source is already an S3 URI — skipping raw upload")

    # Step 1 — Extract
    logger.info("\nSTEP 1: EXTRACT")
    raw_df    = extract(extract_source)
    raw_count = len(raw_df)

    # Step 2 — Standardize formats
    logger.info("\n STEP 2: STANDARDIZE FORMATS")
    df = standardize_formats(raw_df.copy())

    # Step 3 — Clean missing values
    logger.info("\n STEP 3: CLEAN MISSING VALUES")
    df = clean_missing_values(df)

    # Step 4 — Remove duplicates
    logger.info("\n STEP 4: REMOVE DUPLICATES")
    df = remove_duplicates(df)

    # Step 5 — Validate constraints
    logger.info("\n STEP 5: VALIDATE CONSTRAINTS")
    clean_df, rejected_df = validate_constraints(df)

    # Step 6 — Log rejected records
    logger.info("\n STEP 6: LOG REJECTED RECORDS")
    log_rejected_records(rejected_df)

    # Step 7 — Load to database
    logger.info("\n STEP 7: LOAD TO POSTGRESQL")
    load_to_postgres(clean_df, args.db_url, args.table)

    # Step 8 — Upload clean data to S3 (processed zone)
    logger.info("\n STEP 8: UPLOAD CLEAN DATA TO S3")
    upload_clean_to_s3(clean_df, bucket)

    # Final summary
    elapsed = (datetime.now() - start).total_seconds()
    logger.info("\n" + "=" * 65)
    logger.info("  PIPELINE COMPLETE")
    logger.info("=" * 65)
    logger.info(f"  Raw rows extracted   : {raw_count:,}")
    logger.info(f"  Clean rows loaded    : {len(clean_df):,}")
    logger.info(f"  Rejected rows        : {len(rejected_df):,}  "
                f"({len(rejected_df) / raw_count * 100:.1f}%)")
    logger.info(f"  Time elapsed         : {elapsed:.2f}s")
    logger.info(f"  Pipeline log         → {LOG_FILE}")
    logger.info(f"  Rejected records     → logs/rejected_records.csv")
    logger.info("=" * 65)


if __name__ == "__main__":
    main()