-- Works of the checked days, one row per work_id across all days.
--
-- The gate makes work_id unique within a day, but not across days: when OpenAlex changes a
-- work's publication_date, the new version lands in the new day's partition while the old
-- one stays in the old partition (which is never re-fetched once it leaves the lookback
-- window). Keep the newest version; partition_date breaks ties.
select w.*
from {{ silver_table('works') }} as w
inner join {{ ref('stg_quality_days') }} as q using (partition_date)
qualify row_number() over (
    partition by w.work_id
    order by w.updated_date desc, w.partition_date desc
) = 1
