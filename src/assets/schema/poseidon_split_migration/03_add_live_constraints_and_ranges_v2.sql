-- 03_v2) Live table constraints & ranges (fixed ambiguity + immutable generated range)
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:20:04.951739
BEGIN;

DO $$
DECLARE 
  t text;
  v_conname text;
  vf_type text;
  vt_type text;
  range_type text;
  range_func text;
  exists_bool boolean;
BEGIN
  -- Single-key tables
  FOREACH t IN ARRAY ARRAY['flow_definition','rule_definition','cost_node_definition'] LOOP
    v_conname := t || '_reference_key';

    -- Drop legacy strict constraint if present
    EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I DROP CONSTRAINT IF EXISTS %I;', t, t||'_valid_to_null_ck');

    -- Partial unique index on current rows (idempotent)
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes WHERE schemaname = 'schema_poseidon_tst_16_adm' AND indexname = t||'_uniq_current_ref_idx'
    ) THEN
      EXECUTE format('CREATE UNIQUE INDEX %I ON schema_poseidon_tst_16_adm.%I(reference) WHERE is_current=true;', t||'_uniq_current_ref_idx', t);
    END IF;

    -- Ensure UNIQUE(reference) named <table>_reference_key
    -- Check constraint existence using EXECUTE ... USING to avoid ambiguity
    EXECUTE
      'SELECT EXISTS (
         SELECT 1
         FROM pg_constraint c
         JOIN pg_class r ON r.oid = c.conrelid
         JOIN pg_namespace n ON n.oid = r.relnamespace
         WHERE n.nspname = $1 AND r.relname = $2 AND c.conname = $3
       )'
      INTO exists_bool
      USING 'schema_poseidon_tst_16_adm', t, v_conname;

    IF NOT exists_bool THEN
      -- If an index with same name exists, attach using it; else create fresh
      EXECUTE
        'SELECT EXISTS (
           SELECT 1
           FROM pg_class i
           JOIN pg_index ix ON ix.indexrelid = i.oid
           JOIN pg_class r  ON r.oid = ix.indrelid
           JOIN pg_namespace n ON n.oid = r.relnamespace
           WHERE n.nspname = $1 AND r.relname = $2 AND i.relname = $3
         )'
        INTO exists_bool
        USING 'schema_poseidon_tst_16_adm', t, v_conname;

      IF exists_bool THEN
        EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD CONSTRAINT %I UNIQUE USING INDEX %I;', t, v_conname, v_conname);
      ELSE
        EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD CONSTRAINT %I UNIQUE(reference);', t, v_conname);
      END IF;
    END IF;

    -- Detect valid_from/valid_to types to choose immutable range type
    SELECT data_type INTO vf_type
    FROM information_schema.columns
    WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=t AND column_name='valid_from';

    SELECT data_type INTO vt_type
    FROM information_schema.columns
    WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=t AND column_name='valid_to';

    IF vf_type = 'timestamp without time zone' AND (vt_type = 'timestamp without time zone' OR vt_type IS NULL) THEN
      range_type := 'tsrange';
      range_func := 'tsrange';
    ELSE
      range_type := 'tstzrange';
      range_func := 'tstzrange';
    END IF;

    -- Add generated valid_range only if absent
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=t AND column_name='valid_range'
    ) THEN
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD COLUMN valid_range %s GENERATED ALWAYS AS (%s(valid_from, valid_to, ''[)'')) STORED;',
                     t, range_type, range_func);
    END IF;

    -- Proper window check
    BEGIN
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD CONSTRAINT %I CHECK (valid_from < COALESCE(valid_to, ''infinity''::timestamptz));', t, t||'_proper_window_ck');
    EXCEPTION WHEN duplicate_object THEN
      -- ok
    END;
  END LOOP;

  -- Composite-key table
  t := 'cost_allocation_definition';
  -- Drop legacy strict constraint if present
  EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I DROP CONSTRAINT IF EXISTS %I;', t, t||'_valid_to_null_ck');

  -- Partial unique index current rows
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='schema_poseidon_tst_16_adm' AND indexname=t||'_uniq_current_cc_idx'
  ) THEN
    EXECUTE format('CREATE UNIQUE INDEX %I ON schema_poseidon_tst_16_adm.%I(cost_node_reference, cost_component_reference) WHERE is_current=true;', t||'_uniq_current_cc_idx', t);
  END IF;

  -- Detect types for range
  SELECT data_type INTO vf_type
  FROM information_schema.columns
  WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=t AND column_name='valid_from';

  SELECT data_type INTO vt_type
  FROM information_schema.columns
  WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=t AND column_name='valid_to';

  IF vf_type = 'timestamp without time zone' AND (vt_type = 'timestamp without time zone' OR vt_type IS NULL) THEN
    range_type := 'tsrange';
    range_func := 'tsrange';
  ELSE
    range_type := 'tstzrange';
    range_func := 'tstzrange';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=t AND column_name='valid_range'
  ) THEN
    EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD COLUMN valid_range %s GENERATED ALWAYS AS (%s(valid_from, valid_to, ''[)'')) STORED;',
                   t, range_type, range_func);
  END IF;

  -- Proper window check
  BEGIN
    EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD CONSTRAINT %I CHECK (valid_from < COALESCE(valid_to, ''infinity''::timestamptz));', t, t||'_proper_window_ck');
  EXCEPTION WHEN duplicate_object THEN
    -- ok
  END;
END$$;

COMMIT;
