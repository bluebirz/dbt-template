select
    date,
    country_name,
    avg(new_confirmed) over (
        partition by country_name order by date rows between 6 preceding and current row
    ) as weekly_avg_cases
from {{ ref("covid_stats") }}
where date between {{ var("start_date") }} and {{ var("end_date") }}

