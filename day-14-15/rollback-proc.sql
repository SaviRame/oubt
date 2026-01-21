CREATE OR REPLACE FUNCTION mdm.rollback_version(
  p_record_id bigint,        -- customer_sk (any version row for this customer)
  p_target_version integer,  -- version_number to restore
  p_reason text,
  p_run_by text DEFAULT 'system'
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_customer_id text;
  v_now timestamptz := now();
  v_new_version integer;
BEGIN
  -- Find which customer this record_id belongs to
  SELECT customer_id
  INTO v_customer_id
  FROM mdm.dim_customer
  WHERE customer_sk = p_record_id;

  IF v_customer_id IS NULL THEN
    RAISE EXCEPTION 'rollback_version: record_id % not found', p_record_id;
  END IF;

  -- Ensure target version exists
  IF NOT EXISTS (
    SELECT 1
    FROM mdm.dim_customer
    WHERE customer_id = v_customer_id
      AND version_number = p_target_version
  ) THEN
    RAISE EXCEPTION 'rollback_version: target_version % not found for customer_id %',
      p_target_version, v_customer_id;
  END IF;

  -- Next version number
  SELECT max(version_number) + 1
  INTO v_new_version
  FROM mdm.dim_customer
  WHERE customer_id = v_customer_id;

  -- Expire current row (if any)
  UPDATE mdm.dim_customer
  SET
    effective_to = v_now,
    is_current = false
  WHERE customer_id = v_customer_id
    AND is_current = true;

  -- Insert new current row by copying the target version's attributes
  INSERT INTO mdm.dim_customer (
    customer_id, full_name, email, phone, address_line1, city, state, postal_code, segment,
    record_hash,
    effective_from, effective_to, is_current, version_number,
    change_source, change_reason, created_at, created_by,
    is_approved, approved_by, approved_at, approval_reason
  )
  SELECT
    d.customer_id, d.full_name, d.email, d.phone, d.address_line1, d.city, d.state, d.postal_code, d.segment,
    d.record_hash,
    v_now, timestamptz '9999-12-31 00:00:00+00', true, v_new_version,
    'rollback_procedure',
    format('ROLLBACK to v%s: %s', p_target_version, p_reason),
    now(), p_run_by,
    false, NULL, NULL, NULL
  FROM mdm.dim_customer d
  WHERE d.customer_id = v_customer_id
    AND d.version_number = p_target_version;

END;
$$;


SELECT mdm.rollback_version(
  8,                      -- record_id (customer_sk)
  1,                        -- target_version
  'Wrong address loaded from bad batch',
  'data_steward_1'
);

--- Verify rollback
SELECT
  customer_sk,
  customer_id,
  version_number,
  address_line1,
  is_current,
  change_reason
FROM mdm.dim_customer
WHERE customer_id = 'C002'
ORDER BY version_number;


