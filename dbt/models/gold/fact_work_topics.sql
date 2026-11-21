-- Grain: one row per (work, topic). Each work has up to 3 topics ranked by OpenAlex; rank 1
-- is the primary topic. score is often 0.0 for ranks 2-3, so it is kept rather than used as
-- a filter here.
select
    t.work_id,
    t.topic_id,
    w.publication_date,
    t.rank,
    t.score,
    t.rank = 1 as is_primary
from {{ ref('stg_work_topics') }} as t
inner join {{ ref('stg_works') }} as w using (work_id)
