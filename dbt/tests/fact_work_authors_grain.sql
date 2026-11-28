-- The fact's grain: an author slot appears once per work. (The same author_id may fill two
-- slots, so that pair is deliberately not checked.)
select work_id, author_order
from {{ ref('fact_work_authors') }}
group by work_id, author_order
having count(*) > 1
