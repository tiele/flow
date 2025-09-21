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
