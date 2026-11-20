-- No work from a silver day without a _QUALITY_OK marker may reach gold. (The gate checks
-- that publication_date equals the partition day, so the date identifies the partition.)
select work_id, publication_date
from {{ ref('dim_works') }}
where publication_date not in (select partition_date from {{ ref('stg_quality_days') }})
