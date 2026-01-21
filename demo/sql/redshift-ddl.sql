CREATE SCHEMA IF NOT EXISTS master;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS master.dim_vendor (
    vendor_gk BIGINT,
    canonical_name VARCHAR(512),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    change_reason VARCHAR(32),
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS master.zone_master (
    zone_gk BIGINT,
    location_id INTEGER,
    borough VARCHAR(128),
    zone VARCHAR(256),
    service_zone VARCHAR(128),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    change_reason VARCHAR(32),
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS master.payment_type_ref (
    payment_type_gk BIGINT,
    payment_type INTEGER,
    description VARCHAR(256),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    change_reason VARCHAR(32),
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS master.rate_code_ref (
    rate_code_gk BIGINT,
    ratecode_id INTEGER,
    description VARCHAR(256),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    change_reason VARCHAR(32),
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS gold.fact_trips (
    trip_start_date DATE,
    pickup_zone_gk BIGINT,
    dropoff_zone_gk BIGINT,
    vendor_gk BIGINT,
    payment_type_gk BIGINT,
    rate_code_gk BIGINT,
    trip_count BIGINT,
    total_revenue DOUBLE PRECISION,
    total_fare DOUBLE PRECISION,
    total_tips DOUBLE PRECISION,
    total_distance DOUBLE PRECISION,
    total_duration_minutes DOUBLE PRECISION,
    avg_distance DOUBLE PRECISION,
    avg_duration_minutes DOUBLE PRECISION,
    avg_fare_per_mile DOUBLE PRECISION,
    avg_speed_mph DOUBLE PRECISION,
    avg_tip_percentage DOUBLE PRECISION
);
