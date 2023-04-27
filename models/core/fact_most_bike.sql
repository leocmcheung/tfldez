SELECT r.bike_id, COUNT(*) AS num_rentals, sum(r.duration)/3600 AS duration
FROM tfl_bike_dwh.stg_bike_data AS r
GROUP BY r.bike_id ORDER BY num_rentals DESC
