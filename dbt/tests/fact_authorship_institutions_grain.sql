-- Each (slot, institution) link appears once and belongs to an author slot in gold.
select work_id, author_order, institution_id, 'duplicate link' as problem
from {{ ref('fact_authorship_institutions') }}
group by work_id, author_order, institution_id
having count(*) > 1

union all

select i.work_id, i.author_order, i.institution_id, 'no such author slot' as problem
from {{ ref('fact_authorship_institutions') }} as i
left join {{ ref('fact_work_authors') }} as a using (work_id, author_order)
where a.work_id is null
