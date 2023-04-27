SELECT count(case when r.duration/60 > 30 then 1 end) as over30min
    ,count(case when r.duration/60 <=30 then 1 end) as under30min
FROM tfl_bike_dwh.stg_bike_data AS r
