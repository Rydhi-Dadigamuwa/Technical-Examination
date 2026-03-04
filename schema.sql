-- schema.sql — run once before the pipeline to create tables and indexes
-- psql -U postgres -d hotels -f schema.sql

-- drop existing tables first for a clean setup
DROP TABLE IF EXISTS hotel_listings CASCADE;
DROP TABLE IF EXISTS pipeline_audit  CASCADE;


-- main table that stores cleaned hotel listings
CREATE TABLE hotel_listings (
    uniq_id             VARCHAR(64)     PRIMARY KEY,

    property_id         VARCHAR(64)     NOT NULL,
    property_name       VARCHAR(255)    NOT NULL,
    property_type       VARCHAR(50)     DEFAULT 'Unknown',

    country             VARCHAR(100)    DEFAULT 'India',
    state               VARCHAR(100),
    city                VARCHAR(100)    NOT NULL,
    area                VARCHAR(150),
    property_address    TEXT,
    latitude            NUMERIC(9, 6)   CHECK (latitude  BETWEEN 5.0  AND 40.0),
    longitude           NUMERIC(9, 6)   CHECK (longitude BETWEEN 65.0 AND 98.0),

    hotel_star_rating   NUMERIC(2, 1)   CHECK (hotel_star_rating BETWEEN 0 AND 5),
    is_value_plus       SMALLINT        DEFAULT 0 CHECK (is_value_plus IN (0, 1)),

    mmt_review_rating   VARCHAR(100),
    mmt_review_score    NUMERIC(3, 1)   CHECK (mmt_review_score BETWEEN 0 AND 5),
    mmt_review_count    INTEGER         DEFAULT 0 CHECK (mmt_review_count >= 0),
    mmt_location_rating NUMERIC(3, 1)   CHECK (mmt_location_rating BETWEEN 0 AND 5),

    mmt_holidayiq_review_count  INTEGER DEFAULT 0,
    mmt_tripadvisor_count       INTEGER DEFAULT 0,
    site_review_rating          NUMERIC(3, 1),
    site_review_count           INTEGER DEFAULT 0,

    crawl_date          DATE,
    query_time_stamp    TIMESTAMP,

    loaded_at           TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE hotel_listings IS
    'Cleaned MakeMyTrip hotel listings — loaded by ETL pipeline (run_pipeline.py)';


-- indexes to speed up the queries in queries.sql

CREATE INDEX idx_property_type   ON hotel_listings (property_type);
CREATE INDEX idx_crawl_date      ON hotel_listings (crawl_date);
CREATE INDEX idx_city_state      ON hotel_listings (city, state);
CREATE INDEX idx_star_rating     ON hotel_listings (hotel_star_rating);
CREATE INDEX idx_mmt_review_score ON hotel_listings (mmt_review_score);
CREATE INDEX idx_lat_lon         ON hotel_listings (latitude, longitude);
CREATE INDEX idx_type_star       ON hotel_listings (property_type, hotel_star_rating);


-- tracks each pipeline run for monitoring purposes
CREATE TABLE pipeline_audit (
    run_id          SERIAL          PRIMARY KEY,
    run_at          TIMESTAMP       DEFAULT NOW(),
    source_file     VARCHAR(500),
    raw_row_count   INTEGER,
    clean_row_count INTEGER,
    rejected_count  INTEGER,
    elapsed_seconds NUMERIC(8, 2),
    status          VARCHAR(20)     DEFAULT 'success'
);

COMMENT ON TABLE pipeline_audit IS
    'One row per ETL pipeline execution — used for monitoring and alerting';
