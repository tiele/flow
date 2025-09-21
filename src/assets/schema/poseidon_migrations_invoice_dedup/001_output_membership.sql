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
