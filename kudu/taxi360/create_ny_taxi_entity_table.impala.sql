CREATE EXTERNAL TABLE ny_taxi_entity (
  vender_id STRING,
  total_trips INT,
  total_passengers INT,
  total_distance_of_trips DOUBLE,
  max_distance_of_trip DOUBLE,
  total_credit_card_fare_amount DOUBLE,
  total_create_card_extra DOUBLE,
  total_credit_card_mta_tax DOUBLE,
  total_credit_card_impr_surcharge DOUBLE,
  total_credit_card_tip_amount DOUBLE,
  total_credit_card_tolls_amount DOUBLE,
  total_credit_card_total_amount DOUBLE,
  total_cash_fare_amount DOUBLE,
  total_cash_extra DOUBLE,
  total_cash_mta_tax DOUBLE,
  total_cash_impr_surcharge DOUBLE,
  total_cash_tip_amount DOUBLE,
  total_cash_tolls_amount DOUBLE,
  total_cash_total_amount DOUBLE,
  total_credit_card_trips INT,
  total_cash_trips INT,
  total_no_charge_trips INT,
  total_dispute_trips INT,
  total_unknown_trips INT,
  total_voided_trips INT
)
DISTRIBUTE BY HASH (vender_id) INTO 3 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'ny_taxi_entity',
  'kudu.master_addresses' = '<ReplaceMeWithKuduMasterHost>:7051',
  'kudu.key_columns' = 'vender_id'
);