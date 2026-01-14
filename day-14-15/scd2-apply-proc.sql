CREATE OR REPLACE FUNCTION mdm.apply_scd2_customer(
  p_batch_id text,
  p_run_by text DEFAULT 'system',
  p_change_reason text DEFAULT 'daily_load'
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_now timestamptz := now();
BEGIN
  WITH s_dedup AS (
    SELECT *
    FROM (
      SELECT
        s.*,
        row_number() OVER (
          PARTITION BY s.batch_id, s.customer_id
          ORDER BY s.ingested_at DESC, s.source_file DESC
        ) AS rn
      FROM mdm.stg_customer s
      WHERE s.batch_id = p_batch_id
    ) x
    WHERE x.rn = 1
  )
  -- 1) Insert brand new customers
  INSERT INTO mdm.dim_customer (
    customer_id, full_name, email, phone, address_line1, city, state, postal_code, segment,
    record_hash,
    effective_from, effective_to, is_current, version_number,
    change_source, change_reason, created_by
  )
  SELECT
    s.customer_id, s.full_name, s.email, s.phone, s.address_line1, s.city, s.state, s.postal_code, s.segment,
    s.record_hash,
    s.ingested_at, timestamptz '9999-12-31 00:00:00+00', true, 1,
    s.source_system, p_change_reason, p_run_by
  FROM s_dedup s
  LEFT JOIN mdm.dim_customer d
    ON d.customer_id = s.customer_id
   AND d.is_current = true
  WHERE d.customer_id IS NULL;

  -- 2) Expire changed current rows
  UPDATE mdm.dim_customer d
  SET effective_to = v_now,
      is_current = false
  FROM s_dedup s
  WHERE d.customer_id = s.customer_id
    AND d.is_current = true
    AND coalesce(d.record_hash,'') <> coalesce(s.record_hash,'');

  -- 3) Insert new versions for changed customers
  INSERT INTO mdm.dim_customer (
    customer_id, full_name, email, phone, address_line1, city, state, postal_code, segment,
    record_hash,
    effective_from, effective_to, is_current, version_number,
    change_source, change_reason, created_by
  )
  SELECT
    s.customer_id, s.full_name, s.email, s.phone, s.address_line1, s.city, s.state, s.postal_code, s.segment,
    s.record_hash,
    v_now, timestamptz '9999-12-31 00:00:00+00', true,
    prev.max_version + 1,
    s.source_system, p_change_reason, p_run_by
  FROM s_dedup s
  JOIN (
    SELECT customer_id, max(version_number) AS max_version
    FROM mdm.dim_customer
    GROUP BY customer_id
  ) prev
    ON prev.customer_id = s.customer_id
  WHERE EXISTS (
    SELECT 1
    FROM mdm.dim_customer d
    WHERE d.customer_id = s.customer_id
      AND d.is_current = false
      AND d.effective_to = v_now
  );

END;
$$;

TRUNCATE mdm.dim_customer RESTART IDENTITY;

SELECT mdm.apply_scd2_customer('BATCH_001', 'savitha', 'initial_load');

SELECT mdm.apply_scd2_customer('BATCH_002', 'savitha', 'daily_load');


--testing



SELECT
  customer_id,
  version_number,
  address_line1,
  segment,
  effective_from,
  effective_to,
  is_current
FROM mdm.dim_customer
WHERE customer_id IN ('C002','C003')
ORDER BY customer_id, version_number;



SELECT batch_id, COUNT(*) FROM mdm.stg_customer GROUP BY batch_id;

SELECT customer_id, count(*) AS versions
FROM mdm.dim_customer
GROUP BY customer_id
ORDER BY customer_id;

SELECT
  customer_id,
  version_number,
  address_line1,
  segment,
  effective_from,
  effective_to,
  is_current
FROM mdm.dim_customer
WHERE customer_id IN ('C002','C003')
ORDER BY customer_id, version_number;

SELECT batch_id, COUNT(*) AS rows
FROM mdm.stg_customer
GROUP BY batch_id
ORDER BY batch_id;

select DISTINCT batch_id from mdm.stg_customer;


SELECT customer_id, address_line1, segment
FROM mdm.stg_customer
WHERE customer_id IN ('C002','C003','C006')
ORDER BY customer_id;

SELECT batch_id, customer_id, COUNT(*) AS cnt
FROM mdm.stg_customer
GROUP BY batch_id, customer_id
HAVING COUNT(*) > 1;
