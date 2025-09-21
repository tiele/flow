-- 02) Backfill: move non-current or end-dated rows from base -> *_history (two-step pattern)
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:10:29.827014
BEGIN;

DO $$
DECLARE t text; hist text;
BEGIN
  FOREACH t IN ARRAY ARRAY['flow_definition','rule_definition','cost_node_definition','cost_allocation_definition'] LOOP
    hist := t || '_history';
    IF t <> 'cost_allocation_definition' THEN
      EXECUTE format($SQL$
        WITH to_archive AS (
          SELECT b.* FROM schema_poseidon_tst_16_adm.%I b
          WHERE COALESCE(b.is_current,false)=false OR b.valid_to IS NOT NULL
        ), ins AS (
          INSERT INTO schema_poseidon_tst_16_adm.%I
          SELECT ta.* FROM to_archive ta
          RETURNING hist_id, reference, valid_from, valid_to
        )
        UPDATE schema_poseidon_tst_16_adm.%I h
           SET live_id=b.id,
               archived_at=now(),
               archived_by=current_user::text,
               version_no=COALESCE((SELECT MAX(h2.version_no)+1 FROM schema_poseidon_tst_16_adm.%I h2 WHERE h2.reference=b.reference),1)
          FROM schema_poseidon_tst_16_adm.%I b
          JOIN ins i ON i.reference=b.reference
                    AND i.valid_from IS NOT DISTINCT FROM b.valid_from
                    AND i.valid_to   IS NOT DISTINCT FROM b.valid_to
         WHERE h.hist_id=i.hist_id;
        DELETE FROM schema_poseidon_tst_16_adm.%I b
         WHERE COALESCE(b.is_current,false)=false OR b.valid_to IS NOT NULL;
      $SQL$, t, hist, hist, hist, t, t);
    ELSE
      EXECUTE format($SQL$
        WITH to_archive AS (
          SELECT b.* FROM schema_poseidon_tst_16_adm.%I b
          WHERE COALESCE(b.is_current,false)=false OR b.valid_to IS NOT NULL
        ), ins AS (
          INSERT INTO schema_poseidon_tst_16_adm.%I
          SELECT ta.* FROM to_archive ta
          RETURNING hist_id, cost_node_reference, cost_component_reference, valid_from, valid_to
        )
        UPDATE schema_poseidon_tst_16_adm.%I h
           SET live_id=b.id,
               archived_at=now(),
               archived_by=current_user::text,
               version_no=COALESCE(
                 (SELECT MAX(h2.version_no)+1 FROM schema_poseidon_tst_16_adm.%I h2
                  WHERE h2.cost_node_reference=b.cost_node_reference AND h2.cost_component_reference=b.cost_component_reference),1)
          FROM schema_poseidon_tst_16_adm.%I b
          JOIN ins i ON i.cost_node_reference=b.cost_node_reference
                    AND i.cost_component_reference=b.cost_component_reference
                    AND i.valid_from IS NOT DISTINCT FROM b.valid_from
                    AND i.valid_to   IS NOT DISTINCT FROM b.valid_to
         WHERE h.hist_id=i.hist_id;
        DELETE FROM schema_poseidon_tst_16_adm.%I b
         WHERE COALESCE(b.is_current,false)=false OR b.valid_to IS NOT NULL;
      $SQL$, t, hist, hist, hist, t, t);
    END IF;
  END LOOP;
END$$;

-- Backfill version_no from legacy suffixes
DO $$
DECLARE t text; hist text;
BEGIN
  FOREACH t IN ARRAY ARRAY['flow_definition','rule_definition','cost_node_definition','cost_allocation_definition'] LOOP
    hist := t || '_history';
    IF t <> 'cost_allocation_definition' THEN
      EXECUTE format('UPDATE schema_poseidon_tst_16_adm.%I h SET version_no = COALESCE(version_no, (regexp_match(h.reference, ''-V([0-9]{4})$''))[1]::int, 1);', hist);
    ELSE
      EXECUTE format('UPDATE schema_poseidon_tst_16_adm.%I h SET version_no = COALESCE(version_no, (regexp_match(h.cost_node_reference, ''-V([0-9]{4})$''))[1]::int, 1);', hist);
    END IF;
  END LOOP;
END$$;

COMMIT;
