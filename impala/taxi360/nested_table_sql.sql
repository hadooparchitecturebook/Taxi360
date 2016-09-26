invalidate metadata

--drop table ny_taxi_nested;

select * from ny_taxi_nested

select
  p.*,
  c.*
from
  ny_taxi_nested p,
  p.trip c

select p.vender_id, count(*) from ny_taxi_nested p, p.trip c group by p.vender_id

select
  p.vender_id,
  sum(total_amount),
  avg(total_amount),
  count(total_amount)
from
  ny_taxi_nested p,
  p.trip c
group by p.vender_id