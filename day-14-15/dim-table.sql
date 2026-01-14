DROP TABLE IF EXISTS mdm.dim_customer;

CREATE TABLE mdm.dim_customer (
  customer_sk     bigserial PRIMARY KEY,
  customer_id     text        NOT NULL,

  full_name       text        NULL,
  email           text        NULL,
  phone           text        NULL,
  address_line1   text        NULL,
  city            text        NULL,
  state           text        NULL,
  postal_code     text        NULL,
  segment         text        NULL,

  record_hash     text        NULL,

  -- SCD2 columns
  effective_from  timestamptz NOT NULL,
  effective_to    timestamptz NOT NULL DEFAULT timestamptz '9999-12-31 00:00:00+00',
  is_current      boolean     NOT NULL DEFAULT true,
  version_number  integer     NOT NULL,

  -- Governance / audit
  change_source   text        NULL,
  change_reason   text        NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  created_by      text        NULL,

  -- Approval workflow
  is_approved     boolean     NOT NULL DEFAULT false,
  approved_by     text        NULL,
  approved_at     timestamptz NULL,
  approval_reason text        NULL
);

-- One current record per customer
CREATE UNIQUE INDEX ux_dim_customer_current
ON mdm.dim_customer(customer_id)
WHERE is_current;

-- Fast point-in-time queries
CREATE INDEX ix_dim_customer_asof
ON mdm.dim_customer(customer_id, effective_from, effective_to);

