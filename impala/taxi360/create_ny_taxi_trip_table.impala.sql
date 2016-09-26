CREATE EXTERNAL TABLE ny_taxi_trip (
  vender_id STRING,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
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
STORED AS PARQUET
LOCATION 'usr/root/hive/ny_taxi_trip';