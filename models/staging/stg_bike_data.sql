{{ config(materialized='view') }}

select 
    cast(rental_id as integer) as rental_id,
    cast(bike_id as integer) as bike_id,
    cast(start_date as timestamp) as start_date,
    cast(end_date as timestamp) as end_date,
    cast(startstation_id as integer) as startstation_id,
    cast(startstation_name as string) as startstation_name,
    cast(endstation_id as integer) as endstation_id,
    cast(endstation_name as string) as endstation_name,
    cast(duration as integer) as duration,
    cast(bike_model as string) as bike_model
 from {{
    source('staging', 'bike_raw')
}} 
