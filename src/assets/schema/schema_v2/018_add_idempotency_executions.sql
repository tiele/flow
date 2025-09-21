-- ======================================================================
-- 017_logic_idempotency_addition.sql
--
-- Logic Idempotency + Rule Inspection Convenience
--
-- What this migration does
--   • Adds execution.run_at (default now) and execution.logic_sig
--   • Defines a single, explicit EXECUTION MAP for (flow_reference, run_at):
--       – Interpolates ${template.*} in node IDs and link fields (source/target/id)
--       – Discovers rules 1:1 from the INTERPOLATED node IDs (case-sensitive)
--       – Resolves rule history covering run_at; missing rules => content = {}
--       – Map contains: flow_hist_id, nodes_interpolated[], links_interpolated[], rules[]
--   • logic_sig = SHA-256 hash of the EXECUTION MAP
--   • BEFORE INSERT trigger on execution computes input_sig and logic_sig
--   • SUCCESS idempotency index on (flow_reference, input_sig, logic_sig)
--   • Adds a convenience function to list the FULL rule rows used by a flow at run_at
--     with columns aligned to rule_definition_history (where available).
--
-- Notes
--   • Engine remains unchanged (DB computes signals).
--   • No mapping heuristics: rule ref == interpolated node id, as-is.
--   • Template itself is NOT included in the map; only its interpolation effects are.
-- ======================================================================

begin;

-- ----------------------------------------------------------------------
-- 0) Prereqs
-- ----------------------------------------------------------------------
create extension if not exists pgcrypto;  -- digest()

-- ----------------------------------------------------------------------
-- 1) Columns on execution
-- ----------------------------------------------------------------------
alter table schema_poseidon_tst_16_adm.execution
  add column if not exists logic_sig text,
  add column if not exists run_at   timestamptz default now();

comment on column schema_poseidon_tst_16_adm.execution.run_at is
'Timestamp used to resolve flow_definition_history and rule_definition_history when computing logic_sig.';
comment on column schema_poseidon_tst_16_adm.execution.logic_sig is
'Hash of render_execution_map(flow_reference, run_at): interpolated node IDs/links + resolved rule contents (missing rules → {}).';

-- ----------------------------------------------------------------------
-- 2) Stable JSON hasher
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm._hash_jsonb_stable(p jsonb)
returns text
language sql
immutable
as $$
  select encode(digest(jsonb_strip_nulls(p)::text, 'sha256'), 'hex')
$$;

comment on function schema_poseidon_tst_16_adm._hash_jsonb_stable(jsonb) is
'Hex SHA-256 over jsonb after jsonb_strip_nulls() for deterministic hashing.';

-- ----------------------------------------------------------------------
-- 3) Helper: interpolate ${template.*} inside a single text
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm._interp_with_template(
  p_tmpl jsonb,
  p_txt  text
) returns text
language plpgsql
immutable
as $$
declare
  kk text; vv text;
  out text := p_txt;
begin
  if out is null then
    return null;
  end if;
  for kk, vv in select key, value::text from jsonb_each_text(coalesce(p_tmpl,'{}'::jsonb))
  loop
    out := replace(out, '${template.'||kk||'}', vv);
  end loop;
  return out;
end;
$$;

comment on function schema_poseidon_tst_16_adm._interp_with_template(jsonb,text) is
'Interpolates ${template.*} placeholders in a single text using the provided template jsonb.';

-- ----------------------------------------------------------------------
-- 4) Interpolate node IDs (used for rule discovery AND map display)
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm._interpolate_node_ids(p_content jsonb)
returns text[]
language plpgsql
stable
as $$
declare
  v_tmpl  jsonb := coalesce(p_content->'template','{}'::jsonb);
  v_nodes jsonb := coalesce(p_content->'nodes','[]'::jsonb);
  ids     text[] := '{}';
  n jsonb;
  id_val text;
begin
  for n in select * from jsonb_array_elements(v_nodes)
  loop
    id_val := schema_poseidon_tst_16_adm._interp_with_template(v_tmpl, n->>'id');
    if id_val is not null then
      ids := ids || id_val;
    end if;
  end loop;

  -- Deduplicate + sort deterministically.
  select coalesce(array_agg(x order by x), '{}')
    into ids
  from (select distinct unnest(ids) as x) s;

  return ids;
end;
$$;

comment on function schema_poseidon_tst_16_adm._interpolate_node_ids(jsonb) is
'Interpolates ${template.*} in nodes[].id and returns a sorted unique text[] (used as rule references and shown in the execution map).';

-- ----------------------------------------------------------------------
-- 5) Interpolate links (source, target, id) using the same template
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm._interpolate_links(p_content jsonb)
returns jsonb
language plpgsql
stable
as $$
declare
  v_tmpl  jsonb := coalesce(p_content->'template','{}'::jsonb);
  v_links jsonb := coalesce(p_content->'links','[]'::jsonb);
begin
  return (
    with links as (
      select
        schema_poseidon_tst_16_adm._interp_with_template(v_tmpl, x->>'source') as source,
        schema_poseidon_tst_16_adm._interp_with_template(v_tmpl, x->>'target') as target,
        schema_poseidon_tst_16_adm._interp_with_template(v_tmpl, x->>'id')     as id
      from jsonb_array_elements(v_links) x
    ),
    uniq as (
      select distinct source, target, id from links
    )
    select coalesce(
             jsonb_agg(
               jsonb_build_object('source',source,'target',target,'id',id)
               order by source, target, id
             ),
             '[]'::jsonb
           )
    from uniq
  );
end;
$$;

comment on function schema_poseidon_tst_16_adm._interpolate_links(jsonb) is
'Interpolates ${template.*} in links[].source/target/id and returns a deterministic, deduped, sorted jsonb array.';

-- ----------------------------------------------------------------------
-- 6) EXECUTION MAP renderer (transparent & inspectable)
--    Map includes:
--      • flow_hist_id
--      • flow.nodes_interpolated (sorted unique)
--      • flow.links_interpolated (deterministic)
--      • rules[] with full content (or {} when missing)
--    NOTE: Template is NOT included; only its effects via interpolation are.
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm.render_execution_map(
  p_flow_reference text,
  p_run_at timestamptz default now()
) returns jsonb
language plpgsql
stable
as $$
declare
  v_flow_hist_id bigint;
  v_flow_content jsonb;
  v_nodes_interpolated text[];
  v_links_interpolated jsonb;
  v_rules jsonb := '[]'::jsonb;
  r_ref text;
  r_hist_id bigint;
  r_content jsonb;
begin
  -- Load flow history row at run_at
  select h.hist_id, h.content
    into v_flow_hist_id, v_flow_content
  from schema_poseidon_tst_16_adm.flow_definition_history h
  join schema_poseidon_tst_16_adm.flow_definition_live l on l.id = h.live_id
  where l.reference = p_flow_reference
    and (h.valid_from is null or h.valid_from <= p_run_at::timestamp)
    and (h.valid_to   is null or h.valid_to   >  p_run_at::timestamp)
  order by h.valid_from desc nulls last
  limit 1;

  if v_flow_hist_id is null then
    raise exception 'render_execution_map: no flow_definition_history for % at %', p_flow_reference, p_run_at;
  end if;

  -- Flow section (transparent):
  v_nodes_interpolated := schema_poseidon_tst_16_adm._interpolate_node_ids(v_flow_content);
  v_links_interpolated := schema_poseidon_tst_16_adm._interpolate_links(v_flow_content);

  -- Rules: rule ref == interpolated node id (case-sensitive).
  foreach r_ref in array v_nodes_interpolated loop
    select h.hist_id, h.content
      into r_hist_id, r_content
    from schema_poseidon_tst_16_adm.rule_definition_history h
    join schema_poseidon_tst_16_adm.rule_definition_live l on l.id = h.live_id
    where l.reference = r_ref
      and (h.valid_from is null or h.valid_from <= p_run_at::timestamp)
      and (h.valid_to   is null or h.valid_to   >  p_run_at::timestamp)
    order by h.valid_from desc nulls last
    limit 1;

    if r_hist_id is null then
      v_rules := v_rules || jsonb_build_object(
        'reference', r_ref,
        'hist_id',   null,
        'content',   '{}'::jsonb
      );
    else
      v_rules := v_rules || jsonb_build_object(
        'reference', r_ref,
        'hist_id',   r_hist_id,
        'content',   r_content
      );
    end if;
  end loop;

  v_rules := (
    select coalesce(jsonb_agg(x order by x->>'reference'), '[]'::jsonb)
    from jsonb_array_elements(v_rules) x
  );

  return jsonb_build_object(
    'flow_hist_id', v_flow_hist_id,
    'flow', jsonb_build_object(
      'nodes_interpolated', (select coalesce(jsonb_agg(x order by x),'[]'::jsonb) from unnest(v_nodes_interpolated) x),
      'links_interpolated', v_links_interpolated
    ),
    'rules', v_rules
  );
end;
$$;

comment on function schema_poseidon_tst_16_adm.render_execution_map(text,timestamptz) is
'Execution map for (flow_reference, run_at): interpolated node IDs & links, plus resolved rules with full content (missing rules as {}).';

-- ----------------------------------------------------------------------
-- 7) logic_sig = hash(render_execution_map(...))
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm.compute_logic_sig(
  p_flow_reference text,
  p_run_at timestamptz default now()
) returns text
language sql
stable
as $$
  select schema_poseidon_tst_16_adm._hash_jsonb_stable(
           schema_poseidon_tst_16_adm.render_execution_map(p_flow_reference, p_run_at)
         )
$$;

comment on function schema_poseidon_tst_16_adm.compute_logic_sig(text,timestamptz) is
'Hex hash of the execution map (interpolated nodes/links + rules content; missing rules → {}).';

-- ----------------------------------------------------------------------
-- 8) Convenience: list FULL rule rows used by a flow at run_at
--    • Returns columns aligned with rule_definition_history where available.
--    • For missing rules (no covering history row), content = {}, hist_id = NULL,
--      and the other metadata columns are NULL.
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm.list_flow_rules_full(
  p_flow_reference text,
  p_run_at timestamptz default now()
) returns table (
  reference        text,
  type             text,
  category         text,
  description      text,
  content          jsonb,
  base_reference   text,
  hist_id          bigint,
  version_no       integer,
  valid_from       timestamp without time zone,
  valid_to         timestamp without time zone,
  is_current       boolean,
  creation_user    text,
  creation_date    timestamptz,
  user_comment     text
)
language plpgsql
stable
as $$
declare
  v_map jsonb;
  r jsonb;
  r_ref text;
  r_hist_id bigint;
begin
  -- Reuse the authoritative map so UI/ops see exactly what is hashed.
  v_map := schema_poseidon_tst_16_adm.render_execution_map(p_flow_reference, p_run_at);

  for r in
    select * from jsonb_array_elements(coalesce(v_map->'rules','[]'::jsonb))
  loop
    r_ref := r->>'reference';
    r_hist_id := (r->>'hist_id')::bigint;

    if r_hist_id is null then
      -- Missing rule: return a shaped row with content = {}
      reference      := r_ref;
      type           := null;
      category       := null;
      description    := null;
      content        := '{}'::jsonb;
      base_reference := null;
      hist_id        := null;
      version_no     := null;
      valid_from     := null;
      valid_to       := null;
      is_current     := null;
      creation_user  := null;
      creation_date  := null;
      user_comment   := null;
      return next;
    else
      -- Join the exact history row to mirror rule_definition_history columns
      select
        l.reference,
        l.type,
        l.category,
        l.description,
        h.content,
        l.base_reference,
        h.hist_id,
        h.version_no,
        h.valid_from,
        h.valid_to,
        h.is_current,
        h.creation_user,
        h.creation_date,
        l.user_comment
      into
        reference,
        type,
        category,
        description,
        content,
        base_reference,
        hist_id,
        version_no,
        valid_from,
        valid_to,
        is_current,
        creation_user,
        creation_date,
        user_comment
      from schema_poseidon_tst_16_adm.rule_definition_history h
      join schema_poseidon_tst_16_adm.rule_definition_live    l on l.id = h.live_id
      where h.hist_id = r_hist_id;

      return next;
    end if;
  end loop;
end;
$$;

comment on function schema_poseidon_tst_16_adm.list_flow_rules_full(text,timestamptz) is
'Returns full rule rows actually used by (flow_reference, run_at), mirroring rule_definition_history columns; missing rules yield content={} and NULL metadata.';

-- ----------------------------------------------------------------------
-- 9) Compute input_sig from the NEW execution row (engine unchanged)
--    Priority:
--      a) if NEW.data->''inputs_manifest'' exists, hash it as-is (fine-grained idempotency).
--      b) else if NEW.batch_reference exists: build a batch-level manifest including:
--           - source_type, batch_reference,
--           - usage_batch.content_hash (if available),
--           - aggregated hash of active usage.content_hash in that batch (if any).
--      c) else: minimal fallback manifest (flow_reference, source_type, run_day).
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm._compute_input_sig_from_new_exec(
  p_flow_reference text,
  p_source_type text,
  p_batch_reference text,
  p_run_at timestamptz,
  p_data jsonb
) returns text
language plpgsql
stable
as $$
declare
  manifest jsonb;
  v_batch_hash text;
  v_usage_agg text;
begin
  -- a) explicit inputs_manifest provided by the engine => hash as-is
  if p_data ? 'inputs_manifest' then
    return schema_poseidon_tst_16_adm._hash_jsonb_stable(p_data->'inputs_manifest');
  end if;

  -- b) derive from batch_reference (batch-level idempotency)
  if p_batch_reference is not null then
    select ub.content_hash into v_batch_hash
    from schema_poseidon_tst_16_adm.usage_batch ub
    where ub.reference = p_batch_reference;

    -- aggregate active usage.content_hash for this batch (deterministic)
    select case
             when count(*) = 0 then null
             else encode(digest(string_agg(u.content_hash order by u.content_hash, ''), 'sha256'), 'hex')
           end
      into v_usage_agg
    from schema_poseidon_tst_16_adm.usage u
    where u.batch_reference = p_batch_reference
      and coalesce(u.is_active, true);

    manifest := jsonb_build_object(
                  'source_type',     p_source_type,
                  'batch_reference', p_batch_reference,
                  'usage_batch_content_hash', v_batch_hash,
                  'usage_hash_agg',  v_usage_agg
                );

    return schema_poseidon_tst_16_adm._hash_jsonb_stable(manifest);
  end if;

  -- c) minimal fallback manifest (rare path)
  manifest := jsonb_build_object(
                'flow_reference', p_flow_reference,
                'source_type',    p_source_type,
                'run_day',        to_char(p_run_at at time zone 'UTC','YYYY-MM-DD')
              );
  return schema_poseidon_tst_16_adm._hash_jsonb_stable(manifest);
end;
$$;

comment on function schema_poseidon_tst_16_adm._compute_input_sig_from_new_exec(text,text,text,timestamptz,jsonb) is
'Computes input_sig for a newly inserted execution row. Prefers data.inputs_manifest; else batch-level manifest; else minimal fallback.';

-- ----------------------------------------------------------------------
-- 10) BEFORE INSERT trigger on execution: compute input_sig & logic_sig
-- ----------------------------------------------------------------------
create or replace function schema_poseidon_tst_16_adm.trg_execution_compute_sigs()
returns trigger
language plpgsql
as $$
begin
  if new.run_at is null then
    new.run_at := now();
  end if;

  -- input_sig
  new.input_sig := schema_poseidon_tst_16_adm._compute_input_sig_from_new_exec(
                     new.flow_reference,
                     new.source_type,
                     new.batch_reference,
                     new.run_at,
                     coalesce(new.data, '{}'::jsonb)
                   );

  -- logic_sig (execution map hash: nodes/links interpolated; rules content included)
  new.logic_sig := schema_poseidon_tst_16_adm.compute_logic_sig(
                     new.flow_reference,
                     new.run_at
                   );

  return new;
end;
$$;

drop trigger if exists bi_execution_compute_sigs on schema_poseidon_tst_16_adm.execution;

create trigger bi_execution_compute_sigs
before insert on schema_poseidon_tst_16_adm.execution
for each row
execute function schema_poseidon_tst_16_adm.trg_execution_compute_sigs();

comment on function schema_poseidon_tst_16_adm.trg_execution_compute_sigs() is
'BEFORE INSERT on execution: computes input_sig and logic_sig (engine remains owner of status transitions).';

-- ----------------------------------------------------------------------
-- 11) SUCCESS idempotency: inputs × logic
-- ----------------------------------------------------------------------
-- STEP 11 — SUCCESS idempotency: (flow_reference, input_sig, logic_sig) where status='SUCCESS'

-- 11.a) Drop any prior version of the index (schema-qualified is allowed here)
do $$
begin
  if to_regclass('schema_poseidon_tst_16_adm.uq_exec_success_inputs_logic') is not null then
    execute 'drop index schema_poseidon_tst_16_adm.uq_exec_success_inputs_logic';
  end if;
end$$;

-- 11.b) Create the index (do NOT schema-qualify the index name in CREATE; only the table)
create unique index uq_exec_success_inputs_logic
  on schema_poseidon_tst_16_adm.execution (flow_reference, input_sig, logic_sig)
  where status = 'SUCCESS';

-- 11.c) Add the comment (guarded + schema-qualified so it works regardless of search_path)
do $$
begin
  if to_regclass('schema_poseidon_tst_16_adm.uq_exec_success_inputs_logic') is not null then
    execute $cm$
      comment on index schema_poseidon_tst_16_adm.uq_exec_success_inputs_logic is
      'Prevents duplicate SUCCESS for same flow, inputs, and logic snapshot (interpolated nodes/links + rules content).'
    $cm$;
  end if;
end$$;

-- Flow definition (live + history)
ALTER TABLE schema_poseidon_tst_16_adm.flow_definition_live
  ALTER COLUMN base_reference DROP DEFAULT,
  ALTER COLUMN base_reference SET DEFAULT reference;

ALTER TABLE schema_poseidon_tst_16_adm.flow_definition_history
  ALTER COLUMN base_reference DROP DEFAULT,
  ALTER COLUMN base_reference SET DEFAULT reference;

-- Rule definition (live + history)
ALTER TABLE schema_poseidon_tst_16_adm.rule_definition_live
  ALTER COLUMN base_reference DROP DEFAULT,
  ALTER COLUMN base_reference SET DEFAULT reference;

ALTER TABLE schema_poseidon_tst_16_adm.rule_definition_history
  ALTER COLUMN base_reference DROP DEFAULT,
  ALTER COLUMN base_reference SET DEFAULT reference;

-- Cost node definition (live + history)
ALTER TABLE schema_poseidon_tst_16_adm.cost_node_definition_live
  ALTER COLUMN base_reference DROP DEFAULT,
  ALTER COLUMN base_reference SET DEFAULT reference;

ALTER TABLE schema_poseidon_tst_16_adm.cost_node_definition_history
  ALTER COLUMN base_reference DROP DEFAULT,
  ALTER COLUMN base_reference SET DEFAULT reference;

-- Customer definition (not split)
ALTER TABLE schema_poseidon_tst_16_adm.customer_definition
  ALTER COLUMN base_reference DROP DEFAULT,
  ALTER COLUMN base_reference SET DEFAULT reference;

commit;