-- Each author as of the latest checked day that mentions them. The gate guarantees one
-- record per author within a day; across days OpenAlex may rename an author or add an
-- ORCID, and the newest view wins.
select a.*
from {{ silver_table('authors') }} as a
inner join {{ ref('stg_quality_days') }} as q using (partition_date)
qualify row_number() over (partition by a.author_id order by a.partition_date desc) = 1
