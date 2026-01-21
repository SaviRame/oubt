CREATE OR REPLACE FUNCTION mdm.approve_version(
  p_record_id bigint,   -- customer_sk
  p_approver text,
  p_reason text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE mdm.dim_customer
  SET
    is_approved = true,
    approved_by = p_approver,
    approved_at = now(),
    approval_reason = p_reason
  WHERE customer_sk = p_record_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'approve_version: record_id % not found', p_record_id;
  END IF;
END;
$$;



SELECT mdm.approve_version(
  8,
  'data_steward_1',
  'Validated address change with CRM system'
);

select * from mdm.dim_customer;


select * from mdm.dim_customer where customer_sk = 8;

