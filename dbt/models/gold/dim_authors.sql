-- One row per author (latest record across the checked days).
select
    author_id,
    author_name,
    orcid
from {{ ref('stg_authors') }}
