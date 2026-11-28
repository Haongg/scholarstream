-- Every checked day has a file for every silver table gold reads. A day whose silver predates
-- a table (written before the table existed, yet still marked) would otherwise just drop out
-- of that table's facts without any other test noticing.
{% set tables = ['works', 'work_topics', 'topics', 'work_authorships', 'authorship_institutions', 'authors', 'institutions'] %}
with present as (
    {% for table in tables %}
    select '{{ table }}' as silver_table, {{ partition_date('file') }} as partition_date
    from glob('{{ silver_root() }}/{{ table }}/publication_date=*/{{ table }}.parquet')
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

expected as (
    select t.silver_table, q.partition_date
    from {{ ref('stg_quality_days') }} as q
    cross join (values {% for table in tables %}('{{ table }}'){% if not loop.last %}, {% endif %}{% endfor %}) as t(silver_table)
)

select e.silver_table, e.partition_date
from expected as e
left join present as p using (silver_table, partition_date)
where p.partition_date is null
