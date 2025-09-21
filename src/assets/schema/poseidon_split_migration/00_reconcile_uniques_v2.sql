-- 00_v2) Reconcile UNIQUE(reference) on base tables (no-skip, canonical) — fixed ambiguity
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:14:19.708586
BEGIN;

DO $$
DECLARE
  t text;
  v_conname text;
  con_exists boolean;
  con_ok boolean;
  idx_exists boolean;
  idx_ok boolean;
BEGIN
  FOREACH t IN ARRAY ARRAY['flow_definition','rule_definition','cost_node_definition'] LOOP
    v_conname := t || '_reference_key';

    -- 1) Does the constraint named v_conname exist on table t?
    EXECUTE
      'SELECT EXISTS (
         SELECT 1
         FROM pg_constraint c
         JOIN pg_class r ON r.oid = c.conrelid
         JOIN pg_namespace n ON n.oid = r.relnamespace
         WHERE n.nspname = $1
           AND r.relname = $2
           AND c.conname = $3
       )'
      INTO con_exists
      USING 'schema_poseidon_tst_16_adm', t, v_conname;

    con_ok := false;
    IF con_exists THEN
      -- Is it exactly UNIQUE(reference)?
      EXECUTE
        'SELECT (c.contype = ''u'')
                AND (array_length(c.conkey,1) = 1)
                AND EXISTS (
                      SELECT 1
                      FROM unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord)
                      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum
                      WHERE a.attname = ''reference''
                    )
         FROM pg_constraint c
         JOIN pg_class r ON r.oid = c.conrelid
         JOIN pg_namespace n ON n.oid = r.relnamespace
         WHERE n.nspname = $1 AND r.relname = $2 AND c.conname = $3'
        INTO con_ok
        USING 'schema_poseidon_tst_16_adm', t, v_conname;
    END IF;

    IF con_exists AND NOT con_ok THEN
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I DROP CONSTRAINT %I;', t, v_conname);
      con_exists := false;
    END IF;

    -- 2) If constraint missing, check for an index named v_conname on table t
    IF NOT con_exists THEN
      EXECUTE
        'SELECT EXISTS (
           SELECT 1
           FROM pg_class i
           JOIN pg_index ix ON ix.indexrelid = i.oid
           JOIN pg_class r  ON r.oid = ix.indrelid
           JOIN pg_namespace n ON n.oid = r.relnamespace
           WHERE n.nspname = $1
             AND r.relname = $2
             AND i.relname = $3
         )'
        INTO idx_exists
        USING 'schema_poseidon_tst_16_adm', t, v_conname;

      idx_ok := false;
      IF idx_exists THEN
        EXECUTE
          'SELECT (ix.indisunique = true)
                  AND (array_length(ix.indkey,1) = 1)
                  AND EXISTS (
                        SELECT 1
                        FROM unnest(ix.indkey) WITH ORDINALITY AS u(attnum, ord)
                        JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum = u.attnum
                        WHERE a.attname = ''reference''
                      )
           FROM pg_class i
           JOIN pg_index ix ON ix.indexrelid = i.oid
           JOIN pg_class r  ON r.oid = ix.indrelid
           JOIN pg_namespace n ON n.oid = r.relnamespace
           WHERE n.nspname = $1 AND r.relname = $2 AND i.relname = $3'
          INTO idx_ok
          USING 'schema_poseidon_tst_16_adm', t, v_conname;
      END IF;

      IF idx_exists AND idx_ok THEN
        EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD CONSTRAINT %I UNIQUE USING INDEX %I;', t, v_conname, v_conname);
      ELSE
        IF idx_exists AND NOT idx_ok THEN
          EXECUTE format('DROP INDEX schema_poseidon_tst_16_adm.%I;', v_conname);
        END IF;
        EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD CONSTRAINT %I UNIQUE(reference);', t, v_conname);
      END IF;
    END IF;
  END LOOP;
END$$;

COMMIT;
