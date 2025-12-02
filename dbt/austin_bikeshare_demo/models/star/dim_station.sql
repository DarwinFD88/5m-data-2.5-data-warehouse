WITH stations AS (
    SELECT
        station_id,
        name,
        status,
        location,
        address,
        alternate_name,
        city_asset_number,
        property_type,
        number_of_docks,
        power_type,
        footprint_length,
        footprint_width,
        notes,
        council_district,
        image,
        modified_date
    FROM {{ source('austin_bikeshare', 'bikeshare_stations') }}
),

trip_agg AS (
    SELECT
        station_id,
        SUM(duration_minutes * 60) AS total_duration,
        COUNT(start_station_id) AS total_starts,
        COUNT(end_station_id) AS total_ends
    FROM (
        SELECT
            CAST(start_station_id AS STRING) AS station_id,
            duration_minutes,
            start_station_id,
            NULL AS end_station_id
        FROM {{ source('austin_bikeshare', 'bikeshare_trips') }}

        UNION ALL

        SELECT
            CAST(end_station_id AS STRING) AS station_id,
            duration_minutes,
            NULL AS start_station_id,
            end_station_id
        FROM {{ source('austin_bikeshare', 'bikeshare_trips') }}
    )
    GROUP BY station_id
)

SELECT
    s.*,
    COALESCE(t.total_duration, 0) AS total_duration,
    COALESCE(t.total_starts, 0) AS total_starts,
    COALESCE(t.total_ends, 0) AS total_ends
FROM stations s
LEFT JOIN trip_agg t
    ON CAST(s.station_id AS STRING) = t.station_id
