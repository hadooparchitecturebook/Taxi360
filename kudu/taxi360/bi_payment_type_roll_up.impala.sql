select
  payment_type,
  vender_id,
  sum(passenger_count) sum_passenger_cnt,
  max(passenger_count) max_passenger_cnt,
  avg(passenger_count) avg_passenger_cnt,
  sum(fare_amount) sum_fare_amount_cnt,
  max(fare_amount) max_fare_amount_cnt,
  avg(fare_amount) avg_fare_amount_cnt,
  sum(extra) sum_extra_cnt,
  max(extra) max_extra_cnt,
  avg(extra) avg_extra_cnt,
  sum(mta_tax) sum_mta_tax_cnt,
  max(mta_tax) max_mta_tax_cnt,
  avg(mta_tax) avg_mta_tax_cnt,
  sum(improvement_surcharge) sum_improvement_surcharge_cnt,
  max(improvement_surcharge) max_improvement_surcharge_cnt,
  avg(improvement_surcharge) avg_improvement_surcharge_cnt,
  sum(tip_amount) sum_tip_amount_cnt,
  max(tip_amount) max_tip_amount_cnt,
  avg(tip_amount) avg_tip_amount_cnt,
  sum(tolls_amount) sum_tolls_amount_cnt,
  max(tolls_amount) max_tolls_amount_cnt,
  avg(tolls_amount) avg_tolls_amount_cnt,
  sum(total_amount) sum_total_amount_cnt,
  max(total_amount) max_total_amount_cnt,
  avg(total_amount) avg_total_amount_cnt,
  count(*) trip_cnt
from
  ny_taxi_trip
group by
  payment_type, vender_id;
