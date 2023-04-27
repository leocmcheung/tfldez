SELECT r.startstation_name, COUNT(*) AS num_rentals
FROM tfl_bike_dwh.stg_bike_data AS r
GROUP BY r.startstation_name ORDER BY num_rentals DESC
