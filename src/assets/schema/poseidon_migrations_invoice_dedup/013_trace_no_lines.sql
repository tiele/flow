begin;

drop index if exists uq_exec_flow_output_active;
create unique index uq_exec_flow_output_active
  on schema_poseidon_tst_16_adm.execution(flow_reference, output_reference)
  where usage_reference is null
    and status in ('QUEUED','RUNNING','SUCCESS');

drop index if exists uq_exec_success_by_sig;
create unique index uq_exec_success_by_sig
  on schema_poseidon_tst_16_adm.execution(flow_reference, input_sig)
  where usage_reference is null
    and status = 'SUCCESS';

create or replace function schema_poseidon_tst_16_adm._is_trace_output(p_output text)
returns boolean language sql stable as $$
  select exists (
    select 1
    from schema_poseidon_tst_16_adm.output_definition od
    where od.reference = p_output
      and (od.type = 'TRACE' or od.reference like 'TRACE-%')
  );
$$;

create or replace function schema_poseidon_tst_16_adm.trg_invoice_line_before_ins_trace()
returns trigger language plpgsql as
$$
declare v_is_trace boolean;
begin
  v_is_trace := schema_poseidon_tst_16_adm._is_trace_output(NEW.output_reference);
  if v_is_trace then
    perform schema_poseidon_tst_16_adm._log_event(
      'TRACE_SUPPRESSED_LINE',
      coalesce(NEW.update_user, NEW.creation_user),
      NEW.source_type, NEW.flow_reference, NEW.output_reference, NEW.batch_reference, NEW.reference,
      jsonb_build_object('note','trace run suppressed invoice_line insert')
    );
    return null;
  end if;
  return NEW;
end;
$$;

drop trigger if exists invoice_line_before_ins_trace on schema_poseidon_tst_16_adm.invoice_line;
create trigger invoice_line_before_ins_trace
before insert on schema_poseidon_tst_16_adm.invoice_line
for each row execute function schema_poseidon_tst_16_adm.trg_invoice_line_before_ins_trace();

commit;
