begin;

create or replace function schema_poseidon_tst_16_adm._trace_mode()
returns boolean language sql stable as $$
  select coalesce(current_setting('poseidon.trace_only', true), '') = 'on';
$$;

create or replace function schema_poseidon_tst_16_adm.begin_trace_session(
  p_execution_ref text default null,
  p_usage_refs text[] default null
) returns void language plpgsql as
$$
begin
  perform set_config('poseidon.trace_only','on', false);
  if p_execution_ref is not null then
    perform set_config('poseidon.trace_execution', p_execution_ref, false);
  end if;
  perform schema_poseidon_tst_16_adm._log_event(
    'TRACE_SESSION_BEGIN', null, null, null, null, null, null,
    jsonb_build_object('execution_ref', p_execution_ref, 'usage_refs', p_usage_refs)
  );
end $$;

create or replace function schema_poseidon_tst_16_adm.end_trace_session()
returns void language plpgsql as
$$
declare v_exec text := current_setting('poseidon.trace_execution', true);
begin
  perform set_config('poseidon.trace_only','off', false);
  perform set_config('poseidon.trace_execution','', false);
  perform schema_poseidon_tst_16_adm._log_event(
    'TRACE_SESSION_END', null, null, null, null, null, null,
    jsonb_build_object('execution_ref', v_exec)
  );
end $$;

create or replace function schema_poseidon_tst_16_adm.trg_invoice_line_before_ins_trace()
returns trigger language plpgsql as
$$
declare v_is_session_trace boolean;
declare v_is_output_trace boolean;
begin
  v_is_session_trace := schema_poseidon_tst_16_adm._trace_mode();
  if v_is_session_trace then
    perform schema_poseidon_tst_16_adm._log_event(
      'TRACE_SUPPRESSED_LINE',
      coalesce(NEW.update_user, NEW.creation_user),
      NEW.source_type, NEW.flow_reference, NEW.output_reference, NEW.batch_reference, NEW.reference,
      jsonb_build_object('reason','session_trace','note','invoice_line insert suppressed by session trace mode')
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
      jsonb_build_object('reason','output_trace','note','invoice_line insert suppressed for TRACE output')
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
