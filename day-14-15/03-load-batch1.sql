TRUNCATE mdm.stg_customer_raw;

SELECT aws_s3.table_import_from_s3(
  'mdm.stg_customer_raw',
  '',
  '(format csv, header true)',
  aws_commons.create_s3_uri(
    'day-14-15-scd',
    'customers/customers_2026-01-11.csv',
    'us-east-1'
  )
);

INSERT INTO mdm.stg_customer (
  batch_id, ingested_at, source_file, source_system,
  customer_id, full_name, email, phone,
  address_line1, city, state, postal_code, segment,
  record_hash
)
SELECT
  'BATCH_001',
  timestamptz '2026-01-11 10:00:00+00',
  's3://day-14-15-scd/customers/customers_2026-01-11.csv',
  'crm',
  r.customer_id, r.full_name, r.email, r.phone,
  r.address_line1, r.city, r.state, r.postal_code, r.segment,
  md5(concat_ws('||',
    coalesce(r.full_name,''),
    coalesce(r.email,''),
    coalesce(r.phone,''),
    coalesce(r.address_line1,''),
    coalesce(r.city,''),
    coalesce(r.state,''),
    coalesce(r.postal_code,''),
    coalesce(r.segment,'')
  ))
FROM mdm.stg_customer_raw r;



TRUNCATE mdm.stg_customer;