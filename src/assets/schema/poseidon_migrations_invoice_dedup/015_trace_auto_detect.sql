begin;

create index if not exists idx_exec_trace_running
  on schema_poseidon_tst_16_adm.execution(flow_reference, output_reference)
  where usage_reference is not null and status = 'RUNNING';

create or replace function schema_poseidon_tst_16_adm._is_active_trace_run(
  p_flow   text,
  p_output text
) returns boolean
language sql
stable
as $$
  select exists (
    select 1
      from schema_poseidon_tst_16_adm.execution e
     where e.usage_reference is not null
       and e.status = 'RUNNING'
       and e.flow_reference = p_flow
       and (e.output_reference = p_output or e.output_reference is null)
     limit 1
  );
$$;

create or replace function schema_poseidon_tst_16_adm.trg_invoice_line_before_ins_trace()
returns trigger language plpgsql as
$$
declare
  v_is_trace_run boolean;
  v_is_session_trace boolean;
  v_is_output_trace boolean;
begin
  v_is_trace_run := schema_poseidon_tst_16_adm._is_active_trace_run(NEW.flow_reference, NEW.output_reference);
  if v_is_trace_run then
    perform schema_poseidon_tst_16_adm._log_event(
      'TRACE_SUPPRESSED_LINE',
      coalesce(NEW.update_user, NEW.creation_user),
      NEW.source_type, NEW.flow_reference, NEW.output_reference, NEW.batch_reference, NEW.reference,
      jsonb_build_object('reason','active_trace_run')
    );
    return null;
  end if;

  begin
    v_is_session_trace := coalesce(current_setting('poseidon.trace_only', true), '') = 'on';
  exception when others then
    v_is_session_trace := false;
  end;
  if v_is_session_trace then
    perform schema_poseidon_tst_16_adm._log_event(
      'TRACE_SUPPRESSED_LINE',
      coalesce(NEW.update_user, NEW.creation_user),
      NEW.source_type, NEW.flow_reference, NEW.output_reference, NEW.batch_reference, NEW.reference,
      jsonb_build_object('reason','session_trace')
    );
    return null;
  end if;

  v_is_output_trace := exists (
    select 1
    from schema_poseidon_tst_16_adm.output_definition od
    where od.reference = NEW.output_reference
      and (od.type = 'TRACE' or od.reference like 'TRACE-%')
  );
  if v_is_output_trace then
    perform schema_poseidon_tst_16_adm._log_event(
      'TRACE_SUPPRESSED_LINE',
      coalesce(NEW.update_user, NEW.creation_user),
      NEW.source_type, NEW.flow_reference, NEW.output_reference, NEW.batch_reference, NEW.reference,
      jsonb_build_object('reason','output_trace')
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
