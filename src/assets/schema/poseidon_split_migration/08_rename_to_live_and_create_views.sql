-- 08) Rename base -> *_live, reattach triggers, create views, add INSTEAD OF triggers
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:10:29.827014
BEGIN;

DO $$
DECLARE base text; live_name text; key1 text; key2 text;
BEGIN
  FOREACH base IN ARRAY ARRAY['flow_definition','rule_definition','cost_node_definition','cost_allocation_definition'] LOOP
    live_name := base || '_live';

    -- rename once
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=base)
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=live_name) THEN
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I RENAME TO %I;', base, live_name);
    END IF;

    -- reattach versioning triggers ON *_live
    IF base <> 'cost_allocation_definition' THEN
      EXECUTE format('DROP TRIGGER IF EXISTS %I ON schema_poseidon_tst_16_adm.%I;', 'trg_'||base||'_versioning', live_name);
      EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON schema_poseidon_tst_16_adm.%I FOR EACH ROW EXECUTE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_to_history_fn(''reference'');', 'trg_'||base||'_versioning', live_name);
      key1 := 'reference'; key2 := NULL;
    ELSE
      EXECUTE format('DROP TRIGGER IF EXISTS %I ON schema_poseidon_tst_16_adm.%I;', 'trg_'||base||'_versioning', live_name);
      EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON schema_poseidon_tst_16_adm.%I FOR EACH ROW EXECUTE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_to_history_fn(''cost_node_reference'',''cost_component_reference'');', 'trg_'||base||'_versioning', live_name);
      key1 := 'cost_node_reference'; key2 := 'cost_component_reference';
    END IF;

    -- create/replace views with lazy promotion
    EXECUTE format($SQL$
      CREATE OR REPLACE VIEW schema_poseidon_tst_16_adm.%I AS
      WITH _promote AS (
        SELECT schema_poseidon_tst_16_adm._promote_overdue_generic(%L, %L, %s) AS promoted
      )
      SELECT * FROM schema_poseidon_tst_16_adm.%I;
    $SQL$, base, base, key1, CASE WHEN key2 IS NULL THEN 'NULL' ELSE quote_literal(key2) END, live_name);

    -- INSTEAD OF triggers
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON schema_poseidon_tst_16_adm.%I;', base||'_ins_trg', base);
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON schema_poseidon_tst_16_adm.%I;', base||'_upd_trg', base);
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON schema_poseidon_tst_16_adm.%I;', base||'_del_trg', base);

    EXECUTE format('CREATE TRIGGER %I INSTEAD OF INSERT ON schema_poseidon_tst_16_adm.%I FOR EACH ROW EXECUTE FUNCTION schema_poseidon_tst_16_adm._view_passthrough_trg(%L);',
                   base||'_ins_trg', base, format('schema_poseidon_tst_16_adm.%s', live_name));
    EXECUTE format('CREATE TRIGGER %I INSTEAD OF UPDATE ON schema_poseidon_tst_16_adm.%I FOR EACH ROW EXECUTE FUNCTION schema_poseidon_tst_16_adm._view_passthrough_trg(%L);',
                   base||'_upd_trg', base, format('schema_poseidon_tst_16_adm.%s', live_name));
    EXECUTE format('CREATE TRIGGER %I INSTEAD OF DELETE ON schema_poseidon_tst_16_adm.%I FOR EACH ROW EXECUTE FUNCTION schema_poseidon_tst_16_adm._view_passthrough_trg(%L);',
                   base||'_del_trg', base, format('schema_poseidon_tst_16_adm.%s', live_name));
  END LOOP;
END$$;

COMMIT;
