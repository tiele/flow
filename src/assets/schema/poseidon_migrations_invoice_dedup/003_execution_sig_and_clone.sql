begin;

alter table schema_poseidon_tst_16_adm.execution
  add column if not exists input_sig text,
  add column if not exists cloned_from_output text;

create unique index if not exists uq_exec_flow_output_active
  on schema_poseidon_tst_16_adm.execution(flow_reference, output_reference)
  where status in ('QUEUED','RUNNING','SUCCESS');

create unique index if not exists uq_exec_success_by_sig
  on schema_poseidon_tst_16_adm.execution(flow_reference, input_sig)
  where status = 'SUCCESS';

create or replace function schema_poseidon_tst_16_adm.clone_membership_for_flow(
  p_src_output text, p_dst_output text, p_flow text
) returns integer language plpgsql as $$
declare v_rows int; v_closed bool;
begin
  select closed into v_closed
  from   schema_poseidon_tst_16_adm.output_definition
  where  reference = p_dst_output;
  if coalesce(v_closed,false) then
    raise exception 'Cannot clone into closed output %', p_dst_output;
  end if;

  insert into schema_poseidon_tst_16_adm.output_invoice_line (output_reference, line_reference, line_version_no)
  select p_dst_output, v.reference, v.line_version_no
  from   schema_poseidon_tst_16_adm.output_invoice_line ol
  join   schema_poseidon_tst_16_adm.invoice_line_version v
    on  (v.reference, v.line_version_no) = (ol.line_reference, ol.line_version_no)
  where  ol.output_reference = p_src_output
    and  v.flow_reference    = p_flow
  on conflict (output_reference, line_reference)
  do update set line_version_no = excluded.line_version_no;

  get diagnostics v_rows = row_count;
  return v_rows;
end $$;

commit;
