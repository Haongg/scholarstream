-- Grain: one row per (author slot, institution). A slot may list several institutions or
-- none (about a third have none), so this is a bridge from fact_work_authors to
-- dim_institutions rather than a column on it.
select
    i.work_id,
    i.author_order,
    i.institution_id,
    w.publication_date
from {{ ref('stg_authorship_institutions') }} as i
inner join {{ ref('stg_works') }} as w using (work_id)
