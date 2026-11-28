-- Grain: one row per author slot of a work, (work_id, author_order), in byline order.
-- author_id is null for the ~0.7% of slots OpenAlex could not resolve to an author; the slot
-- is kept so that counting rows per work still gives its number of authors. The same author
-- can fill two slots of one work, so (work_id, author_id) is not a key.
select
    a.work_id,
    a.author_order,
    a.author_id,
    w.publication_date,
    a.author_position,
    a.is_corresponding,
    a.raw_author_name
from {{ ref('stg_work_authorships') }} as a
inner join {{ ref('stg_works') }} as w using (work_id)
