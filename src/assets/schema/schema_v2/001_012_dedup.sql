begin;

create table if not exists schema_poseidon_tst_16_adm.output_invoice_line (
  output_reference text not null,
  line_reference   text not null,
  line_version_no  int  not null,
  primary key (output_reference, line_reference),
  foreign key (output_reference)
    references schema_poseidon_tst_16_adm.output_definition(reference)
);

create index if not exists idx_ol_output
  on schema_poseidon_tst_16_adm.output_invoice_line(output_reference);

create index if not exists idx_ol_output_line
  on schema_poseidon_tst_16_adm.output_invoice_line(output_reference, line_reference, line_version_no);

create or replace function schema_poseidon_tst_16_adm.trg_output_invoice_line_guard()
returns trigger language plpgsql as
$$
declare v_closed bool;
begin
  select closed into v_closed
  from   schema_poseidon_tst_16_adm.output_definition
  where  reference = coalesce(NEW.output_reference, OLD.output_reference);
  if coalesce(v_closed,false) then
    raise exception 'Output % is closed; cannot change membership',
      coalesce(NEW.output_reference, OLD.output_reference);
  end if;
  return coalesce(NEW, OLD);
end $$;

drop trigger if exists output_invoice_line_guard
on schema_poseidon_tst_16_adm.output_invoice_line;

create trigger output_invoice_line_guard
before insert or update or delete on schema_poseidon_tst_16_adm.output_invoice_line
for each row execute function schema_poseidon_tst_16_adm.trg_output_invoice_line_guard();

commit;


begin;

alter table schema_poseidon_tst_16_adm.invoice_line
  add column if not exists content_hash text;

-- Create version table if missing, and normalize its primary key
do $$
declare pkname text;
begin
  if not exists (
    select 1 from information_schema.tables
    where table_schema='schema_poseidon_tst_16_adm' and table_name='invoice_line_version'
  ) then
    execute $CT$
      create table schema_poseidon_tst_16_adm.invoice_line_version
      ( like schema_poseidon_tst_16_adm.invoice_line including all );
    $CT$;
  end if;

  -- Drop whatever primary key exists (copied by LIKE ... INCLUDING ALL), then set our composite PK
  select conname
    into pkname
  from pg_constraint
  where conrelid = 'schema_poseidon_tst_16_adm.invoice_line_version'::regclass
    and contype = 'p'
  limit 1;

  if pkname is not null then
    execute format('alter table schema_poseidon_tst_16_adm.invoice_line_version drop constraint %I', pkname);
  end if;

  alter table schema_poseidon_tst_16_adm.invoice_line_version
    add constraint invoice_line_version_pk primary key (reference, line_version_no);

  create unique index if not exists uq_ilv_content
    on schema_poseidon_tst_16_adm.invoice_line_version(reference, content_hash);
end $$;

create or replace function schema_poseidon_tst_16_adm._hash_invoice_line(
  _row schema_poseidon_tst_16_adm.invoice_line
) returns text language sql immutable as $$
  select md5((
    to_jsonb(_row)
      - array[
          'id','creation_user','creation_date','update_user','update_date',
          'processing_message','metadata',
          'content_hash'
      ]
  )::text);
$$;

create or replace function schema_poseidon_tst_16_adm.trg_invoice_line_after_ins()
returns trigger language plpgsql as
$$
declare
  v_closed bool;
  v_ver    int;
begin
  -- ensure hash on base row (for diagnostics)
  if NEW.content_hash is null then
    update schema_poseidon_tst_16_adm.invoice_line
       set content_hash = schema_poseidon_tst_16_adm._hash_invoice_line(NEW)
     where id = NEW.id;
  end if;

  -- destination must be open
  select closed into v_closed
  from   schema_poseidon_tst_16_adm.output_definition
  where  reference = NEW.output_reference;
  if coalesce(v_closed,false) then
    raise exception 'Cannot add/replace lines in closed output %', NEW.output_reference;
  end if;

  -- compute hash for versioning
  NEW.content_hash := schema_poseidon_tst_16_adm._hash_invoice_line(NEW);

  -- identical version already present?
  select line_version_no
    into v_ver
  from schema_poseidon_tst_16_adm.invoice_line_version
  where reference    = NEW.reference
    and content_hash = NEW.content_hash
  order by line_version_no desc
  limit 1;

  if v_ver is null then
    v_ver := coalesce(
      NEW.line_version_no,
      (select coalesce(max(line_version_no),0)+1
         from schema_poseidon_tst_16_adm.invoice_line_version
        where reference = NEW.reference)
    );

    insert into schema_poseidon_tst_16_adm.invoice_line_version
    (
      id, creation_user, creation_date, update_user, update_date,
      reference, batch_reference, source_type, flow_reference, output_reference,
      provider_org_unit, extraction_timestamp, business_timestamp,
      billing_code_provider, billing_code_consumer, production_resource,
      quantity, unit_cost, total_cost, cost_component_reference,
      cost_component_name, cost_component_type, description, technical_description,
      finance_comment, environment, anomalies, metadata, "function",
      invoicing_cycle_id, billing_item_type, line_version_no, is_active,
      billing_code_id, billing_code_description, customer_id, customer_name,
      product_name, product_group, product_domain, manual_modified_by_user_id,
      justification, processing_message, product_id, product_reference,
      valid_from, valid_to, is_current, versioning_comment, content_hash
    )
    values
    (
      NEW.id, NEW.creation_user, NEW.creation_date, NEW.update_user, NEW.update_date,
      NEW.reference, NEW.batch_reference, NEW.source_type, NEW.flow_reference, NEW.output_reference,
      NEW.provider_org_unit, NEW.extraction_timestamp, NEW.business_timestamp,
      NEW.billing_code_provider, NEW.billing_code_consumer, NEW.production_resource,
      NEW.quantity, NEW.unit_cost, NEW.total_cost, NEW.cost_component_reference,
      NEW.cost_component_name, NEW.cost_component_type, NEW.description, NEW.technical_description,
      NEW.finance_comment, NEW.environment, NEW.anomalies, NEW.metadata, NEW."function",
      NEW.invoicing_cycle_id, NEW.billing_item_type, v_ver, NEW.is_active,
      NEW.billing_code_id, NEW.billing_code_description, NEW.customer_id, NEW.customer_name,
      NEW.product_name, NEW.product_group, NEW.product_domain, NEW.manual_modified_by_user_id,
      NEW.justification, NEW.processing_message, NEW.product_id, NEW.product_reference,
      NEW.valid_from, NEW.valid_to, NEW.is_current, NEW.versioning_comment, NEW.content_hash
    )
    on conflict (reference, content_hash) do nothing;
  end if;

  -- snapshot membership (FK will be in place after this migration completes)
  insert into schema_poseidon_tst_16_adm.output_invoice_line
        (output_reference, line_reference, line_version_no)
  values (NEW.output_reference, NEW.reference, v_ver)
  on conflict (output_reference, line_reference)
  do update set line_version_no = excluded.line_version_no;

  return null;
end;
$$;

drop trigger if exists invoice_line_after_ins on schema_poseidon_tst_16_adm.invoice_line;
create trigger invoice_line_after_ins
after insert on schema_poseidon_tst_16_adm.invoice_line
for each row execute function schema_poseidon_tst_16_adm.trg_invoice_line_after_ins();

-- Add FK from output_invoice_line -> invoice_line_version now that the version table exists
do $$
begin
  if not exists (
    select 1 from information_schema.table_constraints tc
    where tc.table_schema = 'schema_poseidon_tst_16_adm'
      and tc.table_name   = 'output_invoice_line'
      and tc.constraint_type = 'FOREIGN KEY'
      and tc.constraint_name = 'fk_output_invoice_line_version'
  ) then
    alter table schema_poseidon_tst_16_adm.output_invoice_line
      add constraint fk_output_invoice_line_version
      foreign key (line_reference, line_version_no)
      references schema_poseidon_tst_16_adm.invoice_line_version(reference, line_version_no);
  end if;
end $$;

commit;


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




begin;

alter table schema_poseidon_tst_16_adm.usage_batch
  add column if not exists is_active boolean not null default true,
  add column if not exists content_hash text;

alter table schema_poseidon_tst_16_adm."usage"
  add column if not exists is_active boolean not null default true,
  add column if not exists content_hash text;

do $$
begin
  if not exists (
    select 1 from information_schema.tables
    where table_schema='schema_poseidon_tst_16_adm' and table_name='usage_batch_history'
  ) then
    create table schema_poseidon_tst_16_adm.usage_batch_history
      ( like schema_poseidon_tst_16_adm.usage_batch including all );
    alter table schema_poseidon_tst_16_adm.usage_batch_history
      drop constraint if exists usage_batch_pkey,
      add column if not exists archived_at timestamptz default now();
  end if;

  if not exists (
    select 1 from information_schema.tables
    where table_schema='schema_poseidon_tst_16_adm' and table_name='usage_history'
  ) then
    create table schema_poseidon_tst_16_adm.usage_history
      ( like schema_poseidon_tst_16_adm."usage" including all );
    alter table schema_poseidon_tst_16_adm.usage_history
      drop constraint if exists usage_pkey,
      add column if not exists archived_at timestamptz default now();
  end if;
end $$;

create or replace function schema_poseidon_tst_16_adm._hash_usage_batch(
  _row schema_poseidon_tst_16_adm.usage_batch
) returns text language sql immutable as $$
  select md5((to_jsonb(_row) - array['id','creation_user','creation_date','update_user','update_date','content_hash'])::text);
$$;

create or replace function schema_poseidon_tst_16_adm._hash_usage(
  _row schema_poseidon_tst_16_adm."usage"
) returns text language sql immutable as $$
  select md5((to_jsonb(_row) - array['id','creation_user','creation_date','content_hash'])::text);
$$;

create or replace function schema_poseidon_tst_16_adm.trg_usage_batch_before_del()
returns trigger language plpgsql as
$$
begin
  if exists (
    select 1
    from   schema_poseidon_tst_16_adm.invoice_line il
    join   schema_poseidon_tst_16_adm.output_definition od
      on   od.reference = il.output_reference
    where  il.batch_reference = old.reference
      and  od.closed = true
  ) then
    raise exception 'Cannot delete usage_batch %, used by closed outputs', old.reference;
  end if;
  return old;
end $$;

drop trigger if exists usage_batch_before_del on schema_poseidon_tst_16_adm.usage_batch;
create trigger usage_batch_before_del
before delete on schema_poseidon_tst_16_adm.usage_batch
for each row execute function schema_poseidon_tst_16_adm.trg_usage_batch_before_del();

create or replace function schema_poseidon_tst_16_adm.trg_usage_before_del()
returns trigger language plpgsql as
$$
begin
  if exists (
    select 1
    from   schema_poseidon_tst_16_adm.invoice_line il
    join   schema_poseidon_tst_16_adm.output_definition od
      on   od.reference = il.output_reference
    where  il.batch_reference = old.batch_reference
      and  od.closed = true
  ) then
    raise exception 'Cannot delete usage %, its batch is used by closed outputs', old.reference;
  end if;
  return old;
end $$;

drop trigger if exists usage_before_del on schema_poseidon_tst_16_adm."usage";
create trigger usage_before_del
before delete on schema_poseidon_tst_16_adm."usage"
for each row execute function schema_poseidon_tst_16_adm.trg_usage_before_del();

create or replace function schema_poseidon_tst_16_adm.trg_usage_batch_biu()
returns trigger language plpgsql as
$$
declare v_hash text;
begin
  v_hash := schema_poseidon_tst_16_adm._hash_usage_batch(coalesce(NEW,OLD));
  if TG_OP = 'INSERT' then
    NEW.content_hash := v_hash;
    NEW.is_active    := true;
  else
    if OLD.content_hash is distinct from v_hash then
      insert into schema_poseidon_tst_16_adm.usage_batch_history select old.*;
      NEW.content_hash := v_hash;
      NEW.is_active    := true;
    else
      NEW := OLD;  -- idempotent
    end if;
  end if;
  return NEW;
end $$;

drop trigger if exists usage_batch_biu on schema_poseidon_tst_16_adm.usage_batch;
create trigger usage_batch_biu
before insert or update on schema_poseidon_tst_16_adm.usage_batch
for each row execute function schema_poseidon_tst_16_adm.trg_usage_batch_biu();

create or replace function schema_poseidon_tst_16_adm.trg_usage_biu()
returns trigger language plpgsql as
$$
declare v_hash text;
begin
  v_hash := schema_poseidon_tst_16_adm._hash_usage(coalesce(NEW,OLD));
  if TG_OP = 'INSERT' then
    NEW.content_hash := v_hash;
    NEW.is_active    := true;
  else
    if OLD.content_hash is distinct from v_hash then
      insert into schema_poseidon_tst_16_adm.usage_history select old.*;
      NEW.content_hash := v_hash;
      NEW.is_active    := true;
    else
      NEW := OLD;
    end if;
  end if;
  return NEW;
end $$;

drop trigger if exists usage_biu on schema_poseidon_tst_16_adm."usage";
create trigger usage_biu
before insert or update on schema_poseidon_tst_16_adm."usage"
for each row execute function schema_poseidon_tst_16_adm.trg_usage_biu();

commit;


begin;

create or replace view schema_poseidon_tst_16_adm.invoice_lines_by_output as
select v.*
from   schema_poseidon_tst_16_adm.output_invoice_line ol
join   schema_poseidon_tst_16_adm.invoice_line_version v
  on  (v.reference, v.line_version_no) = (ol.line_reference, ol.line_version_no);

commit;


begin;

update schema_poseidon_tst_16_adm.invoice_line l
   set content_hash = schema_poseidon_tst_16_adm._hash_invoice_line(l)
 where content_hash is null;

insert into schema_poseidon_tst_16_adm.invoice_line_version
(
  id, creation_user, creation_date, update_user, update_date,
  reference, batch_reference, source_type, flow_reference, output_reference,
  provider_org_unit, extraction_timestamp, business_timestamp,
  billing_code_provider, billing_code_consumer, production_resource,
  quantity, unit_cost, total_cost, cost_component_reference,
  cost_component_name, cost_component_type, description, technical_description,
  finance_comment, environment, anomalies, metadata, "function",
  invoicing_cycle_id, billing_item_type, line_version_no, is_active,
  billing_code_id, billing_code_description, customer_id, customer_name,
  product_name, product_group, product_domain, manual_modified_by_user_id,
  justification, processing_message, product_id, product_reference,
  valid_from, valid_to, is_current, versioning_comment, content_hash
)
select
  l.id, l.creation_user, l.creation_date, l.update_user, l.update_date,
  l.reference, l.batch_reference, l.source_type, l.flow_reference, l.output_reference,
  l.provider_org_unit, l.extraction_timestamp, l.business_timestamp,
  l.billing_code_provider, l.billing_code_consumer, l.production_resource,
  l.quantity, l.unit_cost, l.total_cost, l.cost_component_reference,
  l.cost_component_name, l.cost_component_type, l.description, l.technical_description,
  l.finance_comment, l.environment, l.anomalies, l.metadata, l."function",
  l.invoicing_cycle_id, l.billing_item_type,
  coalesce(l.line_version_no, 1) as line_version_no,
  l.is_active, l.billing_code_id, l.billing_code_description, l.customer_id, l.customer_name,
  l.product_name, l.product_group, l.product_domain, l.manual_modified_by_user_id,
  l.justification, l.processing_message, l.product_id, l.product_reference,
  l.valid_from, l.valid_to, l.is_current, l.versioning_comment, l.content_hash
from schema_poseidon_tst_16_adm.invoice_line l
on conflict (reference, content_hash) do nothing;

insert into schema_poseidon_tst_16_adm.output_invoice_line (output_reference, line_reference, line_version_no)
select l.output_reference, l.reference, coalesce(l.line_version_no, 1)
from   schema_poseidon_tst_16_adm.invoice_line l
where  l.output_reference is not null
on conflict (output_reference, line_reference) do nothing;

commit;


begin;

create table if not exists schema_poseidon_tst_16_adm.event_log (
  id                bigserial primary key,
  ts                timestamptz not null default now(),
  event_type        text not null,
  actor             text,
  source_type       text,
  flow_reference    text,
  output_reference  text,
  batch_reference   text,
  usage_reference   text,
  details           jsonb
);

create index if not exists idx_event_log_ts on schema_poseidon_tst_16_adm.event_log(ts);
create index if not exists idx_event_log_type on schema_poseidon_tst_16_adm.event_log(event_type);
create index if not exists idx_event_log_output on schema_poseidon_tst_16_adm.event_log(output_reference);
create index if not exists idx_event_log_batch on schema_poseidon_tst_16_adm.event_log(batch_reference);

commit;


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


begin;

create or replace function schema_poseidon_tst_16_adm.compute_input_sig(
  p_flow text, p_output text
) returns text language plpgsql as
$$
declare v_sig text;
begin
  select md5(string_agg(x, '|' order by x))
  into   v_sig
  from (
    select concat_ws(':',
             'FLOW', p_flow,
             'OUT',  p_output,
             'SRC',  coalesce(b.source_type,''),
             'BREF', b.reference,
             'BHASH', coalesce(b.content_hash,'')
           ) as x
    from schema_poseidon_tst_16_adm.usage_batch b
    where b.is_active = true
  ) t;

  return v_sig;
end $$;

commit;


begin;

create or replace function schema_poseidon_tst_16_adm.trg_invoice_line_before_upd()
returns trigger language plpgsql as
$$
begin
  raise exception 'invoice_line is append-only; insert a new row instead';
end $$;

drop trigger if exists invoice_line_before_upd on schema_poseidon_tst_16_adm.invoice_line;
create trigger invoice_line_before_upd
before update on schema_poseidon_tst_16_adm.invoice_line
for each row execute function schema_poseidon_tst_16_adm.trg_invoice_line_before_upd();

commit;


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


begin;

create or replace function schema_poseidon_tst_16_adm.compute_input_sig_for_batches(
  p_flow text,
  p_rule jsonb,
  p_batches text[]
) returns text language plpgsql as
$$
declare v_flow_hash text; v_rule_hash text; v_sig text;
begin
  select md5(coalesce(fd.content::text,'')) into v_flow_hash
  from schema_poseidon_tst_16_adm.flow_definition fd
  where fd.reference = p_flow;

  v_rule_hash := md5(coalesce(p_rule::text,'{}'));

  with batches as (
    select b.reference,
           coalesce(b.content_hash,'') as batch_hash,
           b.source_type
    from schema_poseidon_tst_16_adm.usage_batch b
    where b.reference = any(p_batches)
      and b.is_active = true
  ),
  usage_rollup as (
    select u.batch_reference,
           md5(string_agg(coalesce(u.content_hash,''), '|' order by u.reference)) as usages_hash
    from schema_poseidon_tst_16_adm."usage" u
    where u.batch_reference = any(p_batches)
      and u.is_active = true
    group by u.batch_reference
  ),
  lines as (
    select concat_ws(':',
             'FLOW', p_flow,
             'FLOWC', coalesce(v_flow_hash,''),
             'RULE', v_rule_hash,
             'SRC',  coalesce(b.source_type,''),
             'BREF', b.reference,
             'BHASH', b.batch_hash,
             'UHASH', coalesce(ur.usages_hash,'')
           ) as item
    from batches b
    left join usage_rollup ur on ur.batch_reference = b.reference
  )
  select md5(string_agg(item, '|' order by item)) into v_sig
  from lines;

  return v_sig;
end $$;

create or replace function schema_poseidon_tst_16_adm.compute_input_sig_for_latest(
  p_flow text,
  p_rule jsonb,
  p_cutoff_date date
) returns text language plpgsql as
$$
declare v_batches text[];
begin
  with fd as (
    select source_type from schema_poseidon_tst_16_adm.flow_definition where reference = p_flow
  ),
  ranked as (
    select b.*,
           row_number() over (partition by b.source_reference order by b.extraction_timestamp desc nulls last) as rn
    from schema_poseidon_tst_16_adm.usage_batch b, fd
    where b.is_active = true
      and (fd.source_type is null or b.source_type = fd.source_type)
      and (b.extraction_timestamp::date <= p_cutoff_date)
  ),
  chosen as (
    select reference from ranked where rn = 1
  )
  select array_agg(reference order by reference) into v_batches from chosen;

  return schema_poseidon_tst_16_adm.compute_input_sig_for_batches(p_flow, p_rule, v_batches);
end $$;

commit;
