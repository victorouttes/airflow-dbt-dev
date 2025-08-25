{{ config(
    alias='countries'
) }}

with source as (
    select
        code,
        name,
        cast(area as double) as area,
        cast(population as double) as population
    from {{ source('datalake_bronze', 'countries') }}
    where _is_current = true
)
select * from source