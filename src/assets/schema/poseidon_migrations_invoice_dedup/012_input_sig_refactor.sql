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
