-- Every day from the first to the last publication date in gold, including days without
-- works. Carries the calendar effects the pipeline has to know about: works follow a weekly
-- cycle (weekends ~half of weekdays) and the 1st of a month runs high.
with bounds as (
    select min(publication_date) as first_day, max(publication_date) as last_day
    from {{ ref('dim_works') }}
)

select
    cast(d.day as date) as date_day,
    year(d.day) as year,
    quarter(d.day) as quarter,
    month(d.day) as month,
    day(d.day) as day_of_month,
    isodow(d.day) as iso_day_of_week,
    dayname(d.day) as day_name,
    isodow(d.day) >= 6 as is_weekend,
    day(d.day) = 1 as is_month_start
from bounds,
    range(bounds.first_day, bounds.last_day + interval 1 day, interval 1 day) as d(day)
