-- The classifier's label is the primary topic, so every work needs exactly one.
select w.work_id, count(f.work_id) as primary_topics
from {{ ref('dim_works') }} as w
left join {{ ref('fact_work_topics') }} as f
    on f.work_id = w.work_id and f.is_primary
group by w.work_id
having count(f.work_id) != 1
