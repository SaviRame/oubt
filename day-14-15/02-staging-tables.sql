

-- Raw table: structure must match CSV exactly
DROP TABLE IF EXISTS mdm.stg_customer_raw;
CREATE TABLE mdm.stg_customer_raw (
  customer_id   text,
  full_name     text,
  email         text,
  phone         text,
  address_line1 text,
  city          text,
  state         text,
  postal_code   text,
  segment       text
);

-- Staging table: adds ingestion + governance metadata
DROP TABLE IF EXISTS mdm.stg_customer;
CREATE TABLE mdm.stg_customer (
  batch_id      text        NOT NULL,
  ingested_at   timestamptz NOT NULL,
  source_file   text        NOT NULL,
  source_system text        NOT NULL,

  customer_id   text        NOT NULL,
  full_name     text,
  email         text,
  phone         text,
  address_line1 text,
  city          text,
  state         text,
  postal_code   text,
  segment       text,

  record_hash   text
);




