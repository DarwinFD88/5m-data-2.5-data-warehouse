# Assignment

## Brief

Write the SQL statements for the following questions.

## Instructions

Paste the answer as SQL in the answer code section below each question.

### Question 1

Let's revisit our `austin_bikeshare_demo` dbt project. Modify the `dim_station.sql` model to include the following columns:

- `total_duration` (sum of `duration` for each station in seconds)
- `total_starts` (count of `start_station_name` for each station)
- `total_ends` (count of `end_station_name` for each station)

Then, rebuild the models with the following command to see if the changes are correct:

```bash
dbt run
```

Answer:

Paste the `dim_station.sql` model here:

```sql
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
            0 AS duration_minutes,
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
```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.
