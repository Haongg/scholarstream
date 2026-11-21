-- Topics of the kept version of each work only: joining on the partition as well as the
-- work_id drops the topic rows of superseded versions in other days.
select t.*
from {{ silver_table('work_topics') }} as t
inner join {{ ref('stg_works') }} as w using (work_id, partition_date)
