CREATE OR REPLACE FUNCTION mdm.audit_version_history(
  p_record_id bigint,
  p_start_ts timestamptz,
  p_end_ts timestamptz
)
RETURNS TABLE (
  customer_sk bigint,
  customer_id text,
  version_number integer,
  effective_from timestamptz,
  effective_to timestamptz,
  is_current boolean,
  full_name text,
  email text,
  phone text,
  address_line1 text,
  city text,
  state text,
  postal_code text,
  segment text,
  change_source text,
  change_reason text,
  created_at timestamptz,
  created_by text,
  is_approved boolean,
  approved_by text,
  approved_at timestamptz,
  approval_reason text
)
LANGUAGE sql
AS $$
  WITH k AS (
    SELECT customer_id
    FROM mdm.dim_customer
    WHERE customer_sk = p_record_id
  )
  SELECT
    d.customer_sk, d.customer_id, d.version_number,
    d.effective_from, d.effective_to, d.is_current,
    d.full_name, d.email, d.phone, d.address_line1, d.city, d.state, d.postal_code, d.segment,
    d.change_source, d.change_reason,
    d.created_at, d.created_by,
    d.is_approved, d.approved_by, d.approved_at, d.approval_reason
  FROM mdm.dim_customer d
  JOIN k ON k.customer_id = d.customer_id
  WHERE d.effective_from < p_end_ts
    AND d.effective_to   > p_start_ts
  ORDER BY d.version_number;
$$;



SELECT *
FROM mdm.audit_version_history(
  9,
  timestamptz '2026-01-01 00:00:00+00',
  timestamptz '2026-01-31 23:59:59+00'
);
