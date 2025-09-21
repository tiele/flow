-- 01_v2) Create *_history tables with immutable generated valid_range
-- Detects timestamp vs timestamptz and uses tsrange/tstzrange accordingly
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:16:43.957724
BEGIN;

DO $$
DECLARE 
  t text;
  hist text;
  vf_type text;
  vt_type text;
  range_type text;
  range_func text;
BEGIN
  FOREACH t IN ARRAY ARRAY['flow_definition','rule_definition','cost_node_definition','cost_allocation_definition'] LOOP
    hist := t || '_history';

    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=hist
    ) THEN
      EXECUTE format('CREATE TABLE schema_poseidon_tst_16_adm.%I (LIKE schema_poseidon_tst_16_adm.%I INCLUDING DEFAULTS INCLUDING GENERATED);', hist, t);
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD COLUMN hist_id bigserial PRIMARY KEY;', hist);
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD COLUMN live_id int;', hist);
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD COLUMN archived_at timestamptz;', hist);
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD COLUMN archived_by varchar(255);', hist);
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD COLUMN version_no int;', hist);
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD CONSTRAINT %I FOREIGN KEY(live_id) REFERENCES schema_poseidon_tst_16_adm.%I(id) ON DELETE SET NULL;', hist, hist||'_live_id_fkey', t);
    END IF;

    -- Discover data types of valid_from/valid_to in the base table
    SELECT data_type INTO vf_type
    FROM information_schema.columns
    WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=t AND column_name='valid_from';

    SELECT data_type INTO vt_type
    FROM information_schema.columns
    WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=t AND column_name='valid_to';

    -- Choose matching range type and constructor
    IF vf_type = 'timestamp without time zone' AND (vt_type = 'timestamp without time zone' OR vt_type IS NULL) THEN
      range_type := 'tsrange';
      range_func := 'tsrange';
    ELSE
      -- default to timestamptz pairing (immutable)
      range_type := 'tstzrange';
      range_func := 'tstzrange';
    END IF;

    -- Add generated valid_range only if absent
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='schema_poseidon_tst_16_adm' AND table_name=hist AND column_name='valid_range'
    ) THEN
      EXECUTE format('ALTER TABLE schema_poseidon_tst_16_adm.%I ADD COLUMN valid_range %s GENERATED ALWAYS AS (%s(valid_from, valid_to, ''[)'')) STORED;',
                     hist, range_type, range_func);
    END IF;

    -- Helpful indexes
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON schema_poseidon_tst_16_adm.%I(valid_from);', hist||'_vf_idx', hist);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON schema_poseidon_tst_16_adm.%I(valid_to);',   hist||'_vt_idx', hist);
    IF t <> 'cost_allocation_definition' THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON schema_poseidon_tst_16_adm.%I(reference);', hist||'_ref_idx', hist);
    ELSE
      EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON schema_poseidon_tst_16_adm.%I(cost_node_reference, cost_component_reference);', hist||'_cc_idx', hist);
    END IF;
  END LOOP;
END$$;

COMMIT;
