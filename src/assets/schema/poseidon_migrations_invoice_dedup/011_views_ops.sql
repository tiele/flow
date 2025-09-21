begin;

create or replace view schema_poseidon_tst_16_adm.output_contents_by_source as
select
  ol.output_reference,
  v.source_type,
  count(*) as lines,
  min(v.valid_from) as first_valid_from,
  max(v.valid_from) as last_valid_from
from schema_poseidon_tst_16_adm.output_invoice_line ol
join schema_poseidon_tst_16_adm.invoice_line_version v
  on (v.reference, v.line_version_no) = (ol.line_reference, ol.line_version_no)
group by 1,2;

create or replace view schema_poseidon_tst_16_adm.latest_usage_batch_per_source as
with ranked as (
  select b.*,
         row_number() over(partition by b.source_reference order by b.creation_date desc) as rn
  from schema_poseidon_tst_16_adm.usage_batch b
  where b.is_active = true
)
select * from ranked where rn = 1;

commit;
