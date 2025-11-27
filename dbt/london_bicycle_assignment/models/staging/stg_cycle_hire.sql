SELECT
  -- Primary key
  rental_id,
  
  -- Trip attributes
  duration,
  
  -- Start location
  start_station_id,
  start_station_name,
  
  -- End location
  end_station_id,
  end_station_name,
  
  -- Timestamps
  start_date,
  
  -- Derived date/time fields
  EXTRACT(DATE FROM start_date) AS trip_date,
  EXTRACT(HOUR FROM start_date) AS start_hour,
  EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week

FROM {{ source('london_bicycles', 'cycle_hire') }}