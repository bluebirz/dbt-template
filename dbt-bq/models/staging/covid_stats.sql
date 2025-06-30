{{ config(materialized='table') }}

with covid_data as (
    select
        date,
        country_name,
        cumulative_confirmed,
        cumulative_recovered,
        new_confirmed,
        population,
        cumulative_persons_fully_vaccinated,
    from {{ source('covid19_open_data', 'covid19_open_data') }}
)

select * from covid_data
{% if var("is_test_run", default=true) %}
  limit 100
{% endif %}
