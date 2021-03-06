-- transactions:
-- - id: Id of CC transaction, voucher or subscription used
-- - expiration: final date that transaction will be valid until.
-- - type: credit card, voucher, or subscription
-- - status: open|completed|rejected|...
-- - depositor: submitter that used it
-- - item: DSpace Item it was used on
-- - currency: currency selected during the submission process
-- - country: country selected during the submission process
-- - voucher code: added during the submission process

CREATE SEQUENCE shoppingcart_seq;

CREATE TABLE shoppingcart
(
  cart_id INTEGER PRIMARY KEY,
  expiration date,
  status VARCHAR(256),
  depositor INTEGER,
  item INTEGER,
  currency VARCHAR(256),
  country VARCHAR(256),
  voucher INTEGER,
  total DOUBLE PRECISION,
  transaction_id VARCHAR(256),
  securetoken VARCHAR(256),
  basic_fee DOUBLE PRECISION,
  no_integ DOUBLE PRECISION,
  surcharge DOUBLE PRECISION,
  journal VARCHAR(256),
  journal_sub BOOL,
  order_date date,
  payment_date date,
  notes VARCHAR(1024)
);

CREATE SEQUENCE voucher_seq;

CREATE TABLE voucher
(
  voucher_id INTEGER PRIMARY KEY,
  creation date,
  status VARCHAR(256),
  code VARCHAR(256),
  customer VARCHAR(256),
  generator INTEGER,
  explanation VARCHAR(256),
  batch_id VARCHAR(256),
  customer_name VARCHAR(256)
);

ALTER TABLE voucher ADD COLUMN batch_id VARCHAR(256);
ALTER TABLE voucher ADD COLUMN customer_name VARCHAR(256);