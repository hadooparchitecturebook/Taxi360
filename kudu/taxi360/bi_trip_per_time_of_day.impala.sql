select * from
(
select
 count(*) trip_count,
 avg(trip_distance) total_trip,
 avg(tpep_dropoff_datetime - tpep_pickup_datetime) total_time,
 avg(trip_distance) / avg(tpep_dropoff_datetime - tpep_pickup_datetime) avg_speed,
 dayofweek(from_unixtime(cast(tpep_pickup_datetime/1000 as bigint),"yyyy-MM-dd")) day_of_week,
 hour(from_unixtime(cast(tpep_pickup_datetime/1000 as bigint),"yyyy-MM-dd HH:mm:ss")) hour_of_day
from
 ny_taxi_trip
group by day_of_week, hour_of_day
) sub
where day_of_week = 3