begin;

create or replace view schema_poseidon_tst_16_adm.invoice_lines_by_output as
select v.*
from   schema_poseidon_tst_16_adm.output_invoice_line ol
join   schema_poseidon_tst_16_adm.invoice_line_version v
  on  (v.reference, v.line_version_no) = (ol.line_reference, ol.line_version_no);

commit;
