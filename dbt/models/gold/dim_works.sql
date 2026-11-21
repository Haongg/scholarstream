-- One row per work. All work types are kept; filtering (e.g. to articles, or out of bulk
-- uploads) is the consumer's choice, so it is a column here, not a WHERE.
select
    work_id,
    title,
    abstract,
    abstract is not null as has_abstract,
    publication_date,
    publication_year,
    type as work_type,
    language,
    cited_by_count,
    is_retracted,
    updated_date
from {{ ref('stg_works') }}
