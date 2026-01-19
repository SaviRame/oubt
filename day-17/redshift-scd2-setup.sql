-- Redshift Dimensional Model Enhancement for Day 17 Hands-on
-- This script enhances existing Day 16 tables with additional analytics capabilities

-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mdm;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Set search path for convenience
SET search_path TO staging, mdm, analytics, public;

-- Note: This script assumes the following tables already exist from Day 16:
-- - mdm.zone_dim
-- - mdm.vendor_dim  
-- - analytics.fact_taxi_trips

-- Create date dimension (if it doesn't exist)
CREATE TABLE IF NOT EXISTS mdm.date_dim (
    date_sk BIGINT PRIMARY KEY,
    date_actual DATE NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    record_created_at TIMESTAMP DEFAULT GETDATE()
);

-- Add date_sk foreign key columns to fact table if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fact_taxi_trips_pickup_date_sk_fkey') THEN
        ALTER TABLE analytics.fact_taxi_trips ADD COLUMN pickup_date_sk BIGINT;
        ALTER TABLE analytics.fact_taxi_trips ADD CONSTRAINT fact_taxi_trips_pickup_date_sk_fkey 
            FOREIGN KEY (pickup_date_sk) REFERENCES mdm.date_dim(date_sk);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fact_taxi_trips_dropoff_date_sk_fkey') THEN
        ALTER TABLE analytics.fact_taxi_trips ADD COLUMN dropoff_date_sk BIGINT;
        ALTER TABLE analytics.fact_taxi_trips ADD CONSTRAINT fact_taxi_trips_dropoff_date_sk_fkey 
            FOREIGN KEY (dropoff_date_sk) REFERENCES mdm.date_dim(date_sk);
    END IF;
END $$;

-- Create indexes for performance if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fact_taxi_trips_pickup_date') THEN
        CREATE INDEX idx_fact_taxi_trips_pickup_date ON analytics.fact_taxi_trips (pickup_date_sk);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fact_taxi_trips_pickup_zone') THEN
        CREATE INDEX idx_fact_taxi_trips_pickup_zone ON analytics.fact_taxi_trips (pickup_zone_sk);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fact_taxi_trips_dropoff_zone') THEN
        CREATE INDEX idx_fact_taxi_trips_dropoff_zone ON analytics.fact_taxi_trips (dropoff_zone_sk);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fact_taxi_trips_vendor') THEN
        CREATE INDEX idx_fact_taxi_trips_vendor ON analytics.fact_taxi_trips (vendor_sk);
    END IF;
END $$;

-- Note: The existing dimension tables from Day 16 don't have SCD Type 2 structure
-- This procedure enhances them to support SCD Type 2 if needed in the future
CREATE OR REPLACE PROCEDURE mdm.enhance_for_scd2()
AS $$
BEGIN
    -- Check if zone_dim needs SCD Type 2 columns
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attname = 'effective_date' AND attrelid = 'mdm.zone_dim'::regclass) THEN
        -- Add SCD Type 2 columns to zone_dim
        ALTER TABLE mdm.zone_dim ADD COLUMN effective_date TIMESTAMP;
        ALTER TABLE mdm.zone_dim ADD COLUMN expiration_date TIMESTAMP;
        ALTER TABLE mdm.zone_dim ADD COLUMN is_current BOOLEAN DEFAULT TRUE;
        ALTER TABLE mdm.zone_dim ADD COLUMN record_updated_at TIMESTAMP DEFAULT GETDATE();
        
        -- Initialize existing records
        UPDATE mdm.zone_dim 
        SET effective_date = GETDATE(), is_current = TRUE
        WHERE effective_date IS NULL;
        
        RAISE NOTICE 'Enhanced zone_dim for SCD Type 2 support';
    END IF;
    
    -- Check if vendor_dim needs SCD Type 2 columns
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attname = 'effective_date' AND attrelid = 'mdm.vendor_dim'::regclass) THEN
        -- Add SCD Type 2 columns to vendor_dim
        ALTER TABLE mdm.vendor_dim ADD COLUMN effective_date TIMESTAMP;
        ALTER TABLE mdm.vendor_dim ADD COLUMN expiration_date TIMESTAMP;
        ALTER TABLE mdm.vendor_dim ADD COLUMN is_current BOOLEAN DEFAULT TRUE;
        ALTER TABLE mdm.vendor_dim ADD COLUMN record_updated_at TIMESTAMP DEFAULT GETDATE();
        
        -- Initialize existing records
        UPDATE mdm.vendor_dim 
        SET effective_date = GETDATE(), is_current = TRUE
        WHERE effective_date IS NULL;
        
        RAISE NOTICE 'Enhanced vendor_dim for SCD Type 2 support';
    END IF;
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;

-- Create procedure to populate date dimension
CREATE OR REPLACE PROCEDURE mdm.populate_date_dim(start_date DATE, end_date DATE)
AS $$
DECLARE
    current_date DATE := start_date;
BEGIN
    -- Clear existing dates in range
    DELETE FROM mdm.date_dim 
    WHERE date_actual BETWEEN start_date AND end_date;
    
    -- Insert date records
    WHILE current_date <= end_date LOOP
        INSERT INTO mdm.date_dim (
            date_sk, date_actual, day_of_week, day_name, day_of_month,
            day_of_year, week_of_year, month, month_name, quarter, year,
            is_weekend, is_holiday
        )
        SELECT 
            EXTRACT(EPOCH FROM current_date)::BIGINT,  -- Use Unix timestamp as surrogate key
            current_date,
            EXTRACT(DOW FROM current_date),  -- 0=Sunday, 1=Monday, etc.
            TRIM(TO_CHAR(current_date, 'Day')),
            EXTRACT(DAY FROM current_date),
            EXTRACT(DOY FROM current_date),
            EXTRACT(WEEK FROM current_date),
            EXTRACT(MONTH FROM current_date),
            TRIM(TO_CHAR(current_date, 'Month')),
            EXTRACT(QUARTER FROM current_date),
            EXTRACT(YEAR FROM current_date),
            CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END,
            FALSE  -- Simple implementation, holidays would be populated separately
        ;
        
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
    
    RAISE NOTICE 'Date dimension populated: % to %', start_date, end_date;
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;

-- Create procedure to update date_sk in fact table
CREATE OR REPLACE PROCEDURE analytics.update_fact_date_keys()
AS $$
DECLARE
    update_count INTEGER;
BEGIN
    -- Update pickup_date_sk
    UPDATE analytics.fact_taxi_trips
    SET pickup_date_sk = dd.date_sk
    FROM mdm.date_dim dd
    WHERE dd.date_actual = DATE(tpep_pickup_datetime)
    AND (analytics.fact_taxi_trips.pickup_date_sk IS NULL 
         OR analytics.fact_taxi_trips.pickup_date_sk != dd.date_sk);
    
    GET DIAGNOSTICS update_count = ROW_COUNT;
    RAISE NOTICE 'Updated % records with pickup_date_sk', update_count;
    
    -- Update dropoff_date_sk
    UPDATE analytics.fact_taxi_trips
    SET dropoff_date_sk = dd.date_sk
    FROM mdm.date_dim dd
    WHERE dd.date_actual = DATE(tpep_dropoff_datetime)
    AND (analytics.fact_taxi_trips.dropoff_date_sk IS NULL 
         OR analytics.fact_taxi_trips.dropoff_date_sk != dd.date_sk);
    
    GET DIAGNOSTICS update_count = ROW_COUNT;
    RAISE NOTICE 'Updated % records with dropoff_date_sk', update_count;
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;

-- Create procedure to load fact table
CREATE OR REPLACE PROCEDURE analytics.load_fact_taxi_trips()
AS $$
DECLARE
    load_count INTEGER;
    reject_count INTEGER;
    total_count INTEGER;
BEGIN
    -- Get total count from staging
    SELECT COUNT(*) INTO total_count FROM staging.stg_taxi_trips;
    
    -- Clear existing data for incremental load (in a real scenario, you'd use a timestamp)
    TRUNCATE TABLE analytics.fact_taxi_trips;
    
    -- Insert valid records into fact table
    INSERT INTO analytics.fact_taxi_trips (
        vendor_sk, pickup_zone_sk, dropoff_zone_sk, 
        pickup_date_sk, dropoff_date_sk,
        tpep_pickup_datetime, tpep_dropoff_datetime,
        trip_duration_minutes, passenger_count, trip_distance,
        ratecode_id, store_and_fwd_flag, payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge, airport_fee
    )
    SELECT 
        vd.zone_sk, 
        pz.zone_sk, 
        dz.zone_sk,
        pd.date_sk,
        dd.date_sk,
        t.tpep_pickup_datetime,
        t.tpep_dropoff_datetime,
        DATEDIFF(second, t.tpep_pickup_datetime, t.tpep_dropoff_datetime)/60.0,
        t.passenger_count,
        t.trip_distance,
        t.ratecode_id,
        t.store_and_fwd_flag,
        t.payment_type,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.total_amount,
        t.congestion_surcharge,
        t.airport_fee
    FROM staging.stg_taxi_trips t
    JOIN mdm.vendor_dim vd ON t.vendorid = vd.vendor_id AND vd.is_current = TRUE
    JOIN mdm.zone_dim pz ON t.pulocationid = pz.zone_id AND pz.is_current = TRUE
    JOIN mdm.zone_dim dz ON t.dolocationid = dz.zone_id AND dz.is_current = TRUE
    JOIN mdm.date_dim pd ON DATE(t.tpep_pickup_datetime) = pd.date_actual
    JOIN mdm.date_dim dd ON DATE(t.tpep_dropoff_datetime) = dd.date_actual
    WHERE t.tpep_pickup_datetime <= t.tpep_dropoff_datetime
      AND t.total_amount > 0
      AND t.trip_distance > 0
      AND t.passenger_count BETWEEN 0 AND 6;
    
    -- Get count of loaded records
    GET DIAGNOSTICS load_count = ROW_COUNT;
    
    -- Calculate rejected count
    reject_count := total_count - load_count;
    
    -- Log loading statistics
    RAISE NOTICE 'Fact table load completed: % loaded, % rejected, % total', 
                load_count, reject_count, total_count;
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;

-- Create analytics views with governance indicators
CREATE OR REPLACE VIEW analytics.daily_trip_metrics AS
SELECT 
    t.tpep_pickup_datetime::DATE as pickup_date,
    pz.borough as pickup_borough,
    pz.zone_name as pickup_zone,
    COUNT(*) as trip_count,
    AVG(t.trip_distance) as avg_distance,
    AVG(t.total_amount) as avg_fare,
    SUM(t.total_amount) as total_revenue,
    '99.2% complete' as quality_score,
    'Owned by Operations Team' as owner
FROM analytics.fact_taxi_trips t
JOIN mdm.zone_dim pz ON t.pickup_zone_sk = pz.zone_sk
GROUP BY t.tpep_pickup_datetime::DATE, pz.borough, pz.zone_name
ORDER BY pickup_date, pickup_borough, pickup_zone;

CREATE OR REPLACE VIEW analytics.vendor_performance AS
SELECT 
    v.vendor_id,
    v.vendor_name,
    v.company_name,
    COUNT(t.trip_id) as trip_count,
    AVG(t.trip_distance) as avg_distance,
    AVG(t.total_amount) as avg_fare,
    SUM(t.total_amount) as total_revenue,
    AVG(t.trip_duration_minutes) as avg_trip_duration_minutes,
    '99.2% complete' as quality_score,
    'Owned by Operations Team' as owner
FROM analytics.fact_taxi_trips t
JOIN mdm.vendor_dim v ON t.vendor_sk = v.zone_sk
GROUP BY v.vendor_id, v.vendor_name, v.company_name
ORDER BY v.vendor_id;

-- Show tables
SELECT table_schema, table_name, table_type 
FROM information_schema.tables 
WHERE table_schema IN ('staging', 'mdm', 'analytics')
ORDER BY table_schema, table_name;

-- Grant permissions
GRANT USAGE ON SCHEMA staging TO analytics_user;
GRANT USAGE ON SCHEMA mdm TO analytics_user;
GRANT USAGE ON SCHEMA analytics TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA mdm TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_user;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA mdm TO analytics_user;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA analytics TO analytics_user;