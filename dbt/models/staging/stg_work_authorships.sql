-- Author slots of the kept version of each work only: joining on the partition as well as
-- the work_id drops the slots of superseded versions in other days (as stg_work_topics).
select a.*
from {{ silver_table('work_authorships') }} as a
inner join {{ ref('stg_works') }} as w using (work_id, partition_date)
