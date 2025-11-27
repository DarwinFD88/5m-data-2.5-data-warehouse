{% snapshot snap_cycle_stations %}

{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='removal_date',
    invalidate_hard_deletes=True
  )
}}

SELECT
  id,
  name AS station_name,
  docks_count,
  install_date,
  removal_date

FROM {{ source('london_bicycles', 'cycle_stations') }}

{% endsnapshot %}