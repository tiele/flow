
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
