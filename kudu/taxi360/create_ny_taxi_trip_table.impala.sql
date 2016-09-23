CREATE EXTERNAL TABLE ny_taxi_trip (
  vender_id STRING,
  tpep_pickup_datetime BIGINT,
  tpep_dropoff_datetime BIGINT,
  passenger_count INT,
  trip_distance DOUBLE,
  pickup_longitude DOUBLE,
  pickup_latitude DOUBLE,
  rate_code_id STRING,
  store_and_fwd_flag STRING,
  dropoff_longitude DOUBLE,
  dropoff_latitude DOUBLE,
  payment_type STRING,
  fare_amount DOUBLE,
  extra DOUBLE,
  mta_tax DOUBLE,
  improvement_surcharge DOUBLE,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  total_amount DOUBLE
)
DISTRIBUTE BY HASH (vender_id) INTO 3 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'ny_taxi_trip',
  'kudu.master_addresses' = '<ReplaceMeWithKuduMasterHost>:7051',
  'kudu.key_columns' = 'vender_id, tpep_pickup_datetime'
);