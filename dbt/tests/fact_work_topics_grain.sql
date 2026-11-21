-- The fact's grain: a topic appears once per work, and a rank is used once per work.
select work_id, 'duplicate topic ' || topic_id as problem
from {{ ref('fact_work_topics') }}
group by work_id, topic_id
having count(*) > 1

union all

select work_id, 'duplicate rank ' || rank as problem
from {{ ref('fact_work_topics') }}
group by work_id, rank
having count(*) > 1
