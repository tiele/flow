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
