-- Each institution as of the latest checked day that mentions it (see stg_authors).
select i.*
from {{ silver_table('institutions') }} as i
inner join {{ ref('stg_quality_days') }} as q using (partition_date)
qualify row_number() over (partition by i.institution_id order by i.partition_date desc) = 1
