-- A work with authors has exactly one first author (works without authors are allowed).
select work_id, count(*) filter (where author_position = 'first') as first_authors
from {{ ref('fact_work_authors') }}
group by work_id
having count(*) filter (where author_position = 'first') != 1
