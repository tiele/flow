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
