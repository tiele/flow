begin;

create or replace function schema_poseidon_tst_16_adm._log_event(
  p_type text, p_actor text, p_source_type text, p_flow text, p_output text, p_batch text, p_usage text, p_details jsonb
) returns void language sql as $$
  insert into schema_poseidon_tst_16_adm.event_log
  (event_type, actor, source_type, flow_reference, output_reference, batch_reference, usage_reference, details)
  values ($1,$2,$3,$4,$5,$6,$7,$8);
$$;

create or replace function schema_poseidon_tst_16_adm.trg_usage_batch_evt()
returns trigger language plpgsql as
$$
begin
  if TG_OP = 'INSERT' then
    perform schema_poseidon_tst_16_adm._log_event(
      'USAGE_BATCH_INSERT', new.creation_user, new.source_type, null, null, new.reference, null,
      jsonb_build_object('content_hash', new.content_hash, 'source_reference', new.source_reference, 'extraction_timestamp', new.extraction_timestamp)
    );
  elsif TG_OP = 'UPDATE' then
    if new.content_hash is distinct from old.content_hash then
      perform schema_poseidon_tst_16_adm._log_event(
        'USAGE_BATCH_SUPERSEDE', coalesce(new.update_user, new.creation_user), new.source_type, null, null, new.reference, null,
        jsonb_build_object('old_hash', old.content_hash, 'new_hash', new.content_hash)
      );
    end if;
  end if;
  return new;
end $$;

drop trigger if exists usage_batch_evt on schema_poseidon_tst_16_adm.usage_batch;
create trigger usage_batch_evt
after insert or update on schema_poseidon_tst_16_adm.usage_batch
for each row execute function schema_poseidon_tst_16_adm.trg_usage_batch_evt();

create or replace function schema_poseidon_tst_16_adm.trg_usage_evt()
returns trigger language plpgsql as
$$
begin
  if TG_OP = 'INSERT' then
    perform schema_poseidon_tst_16_adm._log_event(
      'USAGE_INSERT', new.creation_user, null, null, null, new.batch_reference, new.reference,
      jsonb_build_object('content_hash', new.content_hash, 'business_timestamp', new.business_timestamp)
    );
  elsif TG_OP = 'UPDATE' then
    if new.content_hash is distinct from old.content_hash then
      perform schema_poseidon_tst_16_adm._log_event(
        'USAGE_SUPERSEDE', coalesce(new.creation_user, old.creation_user), null, null, null, new.batch_reference, new.reference,
        jsonb_build_object('old_hash', old.content_hash, 'new_hash', new.content_hash)
      );
    end if;
  end if;
  return new;
end $$;

drop trigger if exists usage_evt on schema_poseidon_tst_16_adm."usage";
create trigger usage_evt
after insert or update on schema_poseidon_tst_16_adm."usage"
for each row execute function schema_poseidon_tst_16_adm.trg_usage_evt();

create or replace function schema_poseidon_tst_16_adm.trg_output_def_evt()
returns trigger language plpgsql as
$$
begin
  if TG_OP='UPDATE' and new.closed is distinct from old.closed then
    perform schema_poseidon_tst_16_adm._log_event(
      case when new.closed then 'OUTPUT_CLOSED' else 'OUTPUT_REOPENED' end,
      coalesce(new.update_user, new.creation_user),
      null, null, new.reference, null, null, jsonb_build_object('old', old.closed, 'new', new.closed)
    );
  end if;
  return new;
end $$;

drop trigger if exists output_def_evt on schema_poseidon_tst_16_adm.output_definition;
create trigger output_def_evt
after update on schema_poseidon_tst_16_adm.output_definition
for each row execute function schema_poseidon_tst_16_adm.trg_output_def_evt();

create or replace function schema_poseidon_tst_16_adm.trg_output_membership_evt()
returns trigger language plpgsql as
$$
begin
  if TG_OP='INSERT' then
    perform schema_poseidon_tst_16_adm._log_event(
      'OUTPUT_MEMBERSHIP_INSERT', null, null, null, new.output_reference, null, new.line_reference,
      jsonb_build_object('line_version_no', new.line_version_no)
    );
  elsif TG_OP='UPDATE' then
    perform schema_poseidon_tst_16_adm._log_event(
      'OUTPUT_MEMBERSHIP_UPDATE', null, null, null, new.output_reference, null, new.line_reference,
      jsonb_build_object('old_version', old.line_version_no, 'new_version', new.line_version_no)
    );
  end if;
  return new;
end $$;

drop trigger if exists output_membership_evt on schema_poseidon_tst_16_adm.output_invoice_line;
create trigger output_membership_evt
after insert or update on schema_poseidon_tst_16_adm.output_invoice_line
for each row execute function schema_poseidon_tst_16_adm.trg_output_membership_evt();

create or replace function schema_poseidon_tst_16_adm.trg_ilv_evt()
returns trigger language plpgsql as
$$
begin
  perform schema_poseidon_tst_16_adm._log_event(
    'INVOICE_LINE_VERSION_CREATED',
    coalesce(new.update_user, new.creation_user),
    new.source_type, new.flow_reference, new.output_reference, new.batch_reference, new.reference,
    jsonb_build_object('line_version_no', new.line_version_no, 'content_hash', new.content_hash)
  );
  return new;
end $$;

drop trigger if exists ilv_evt on schema_poseidon_tst_16_adm.invoice_line_version;
create trigger ilv_evt
after insert on schema_poseidon_tst_16_adm.invoice_line_version
for each row execute function schema_poseidon_tst_16_adm.trg_ilv_evt();

create or replace function schema_poseidon_tst_16_adm.trg_execution_evt()
returns trigger language plpgsql as
$$
begin
  if TG_OP='INSERT' then
    perform schema_poseidon_tst_16_adm._log_event(
      'EXECUTION_SCHEDULED', null, null, new.flow_reference, new.output_reference, null, null,
      jsonb_build_object('status', new.status, 'input_sig', new.input_sig, 'usage_reference', new.usage_reference)
    );
  elsif TG_OP='UPDATE' then
    if new.status is distinct from old.status then
      perform schema_poseidon_tst_16_adm._log_event(
        'EXECUTION_STATUS', null, null, new.flow_reference, new.output_reference, null, null,
        jsonb_build_object('from', old.status, 'to', new.status, 'input_sig', new.input_sig, 'cloned_from_output', new.cloned_from_output, 'usage_reference', new.usage_reference)
      );
    end if;
  end if;
  return new;
end $$;

drop trigger if exists execution_evt_ins on schema_poseidon_tst_16_adm.execution;
create trigger execution_evt_ins
after insert on schema_poseidon_tst_16_adm.execution
for each row execute function schema_poseidon_tst_16_adm.trg_execution_evt();

drop trigger if exists execution_evt_upd on schema_poseidon_tst_16_adm.execution;
create trigger execution_evt_upd
after update on schema_poseidon_tst_16_adm.execution
for each row execute function schema_poseidon_tst_16_adm.trg_execution_evt();

commit;
