-- One row per topic with its full hierarchy, denormalised (star, not snowflake):
-- topic -> subfield -> field -> domain.
select
    topic_id,
    topic_name,
    subfield_id,
    subfield_name,
    field_id,
    field_name,
    domain_id,
    domain_name
from {{ ref('stg_topics') }}
