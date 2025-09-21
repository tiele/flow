begin;

drop function if exists schema_poseidon_tst_16_adm.begin_trace_session(text, text[]);
drop function if exists schema_poseidon_tst_16_adm.end_trace_session();
drop function if exists schema_poseidon_tst_16_adm._trace_mode();
drop function if exists schema_poseidon_tst_16_adm._is_trace_output(text);

create or replace function schema_poseidon_tst_16_adm.trg_invoice_line_before_ins_trace()
returns trigger language plpgsql as
$$
begin
  if schema_poseidon_tst_16_adm._is_active_trace_run(NEW.flow_reference, NEW.output_reference) then
    perform schema_poseidon_tst_16_adm._log_event(
      'TRACE_SUPPRESSED_LINE', coalesce(NEW.update_user, NEW.creation_user),
      NEW.source_type, NEW.flow_reference, NEW.output_reference, NEW.batch_reference, NEW.reference,
      jsonb_build_object('reason','active_trace_run')
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
