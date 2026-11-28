-- Institution links of the kept version of each work only (see stg_work_authorships).
select i.*
from {{ silver_table('authorship_institutions') }} as i
inner join {{ ref('stg_works') }} as w using (work_id, partition_date)
