-- One row per institution (latest record across the checked days). country_code can be
-- null; the parent lineage is left out for now.
select
    institution_id,
    institution_name,
    ror,
    country_code,
    institution_type
from {{ ref('stg_institutions') }}
