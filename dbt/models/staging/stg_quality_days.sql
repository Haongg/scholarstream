-- Silver days that passed the quality gate since they were last written: the gate writes
-- works/publication_date=X/_QUALITY_OK, transform deletes it before rewriting the day.
-- Gold reads only these days; a failed or still-settling day stays out until it passes.
select distinct {{ partition_date('file') }} as partition_date
from glob('{{ silver_root() }}/works/publication_date=*/_QUALITY_OK')
