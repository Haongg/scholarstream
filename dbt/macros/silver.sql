{#
  Silver lives in the lake as works/publication_date=YYYY-MM-DD/works.parquet (likewise
  work_topics, topics). These macros read it in place.

  partition_date is the day of the partition a row was read from, taken from the file path.
  It is what the quality marker refers to, so the marker filter joins on it rather than on a
  column inside the file.
#}

{% macro silver_root() -%}
  {{ env_var('SILVER_ROOT', 's3://silver') }}
{%- endmacro %}

{% macro partition_date(path_column) -%}
  cast(regexp_extract({{ path_column }}, 'publication_date=([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) as date)
{%- endmacro %}

{% macro silver_table(table) -%}
  (
    select * exclude (filename), {{ partition_date('filename') }} as partition_date
    from read_parquet(
      '{{ silver_root() }}/{{ table }}/publication_date=*/{{ table }}.parquet',
      filename = true,
      hive_partitioning = false
    )
  )
{%- endmacro %}
