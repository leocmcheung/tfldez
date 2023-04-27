SELECT r.bike_model, COUNT(*) AS num_rentals
FROM tfl_bike_dwh.stg_bike_data AS r
GROUP BY r.bike_model ORDER BY num_rentals DESC
