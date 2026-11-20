-- The topic hierarchy as of the latest checked day that mentions each topic. The gate
-- guarantees one hierarchy per topic within a day; across days OpenAlex may rename or move
-- a topic, and the newest view wins.
select t.*
from {{ silver_table('topics') }} as t
inner join {{ ref('stg_quality_days') }} as q using (partition_date)
qualify row_number() over (partition by t.topic_id order by t.partition_date desc) = 1
