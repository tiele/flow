-- Poseidon FK refresh for live/history split (obvious FKs)
-- Schema: schema_poseidon_tst_16_adm

BEGIN;

-- Safety: make sure the live table exists (post-migration)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'schema_poseidon_tst_16_adm'
      AND table_name   = 'flow_definition_live'
  ) THEN
    RAISE EXCEPTION 'Expected table schema_poseidon_tst_16_adm.flow_definition_live not found. Run the live/history migration first.';
  END IF;
END$$;

-- 1) invoice_line.flow_reference -> flow_definition_live(reference)
DO $$
BEGIN
  -- Drop old FK if it still points to flow_definition (the view after migration)
  IF EXISTS (
    SELECT 1
    FROM information_schema.table_constraints c
    WHERE c.table_schema = 'schema_poseidon_tst_16_adm'
      AND c.table_name   = 'invoice_line'
      AND c.constraint_name = 'invoice_line_flow_reference_fkey'
  ) THEN
    EXECUTE 'ALTER TABLE schema_poseidon_tst_16_adm.invoice_line
             DROP CONSTRAINT invoice_line_flow_reference_fkey';
  END IF;

  -- Create the correct FK to *_live if missing
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.referential_constraints rc
    JOIN information_schema.table_constraints tc
      ON tc.constraint_name = rc.constraint_name
     AND tc.constraint_schema = rc.constraint_schema
    WHERE tc.table_schema = 'schema_poseidon_tst_16_adm'
      AND tc.table_name   = 'invoice_line'
      AND tc.constraint_name = 'invoice_line_flow_reference_fkey'
  ) THEN
    EXECUTE 'ALTER TABLE schema_poseidon_tst_16_adm.invoice_line
             ADD CONSTRAINT invoice_line_flow_reference_fkey
             FOREIGN KEY (flow_reference)
             REFERENCES schema_poseidon_tst_16_adm.flow_definition_live(reference)';
  END IF;
END$$;

-- 2) execution.flow_reference -> flow_definition_live(reference)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE table_schema='schema_poseidon_tst_16_adm'
      AND table_name='execution'
      AND constraint_name='execution_flow_reference_fkey'
  ) THEN
    EXECUTE 'ALTER TABLE schema_poseidon_tst_16_adm.execution
             DROP CONSTRAINT execution_flow_reference_fkey';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.referential_constraints rc
    JOIN information_schema.table_constraints tc
      ON tc.constraint_name = rc.constraint_name
     AND tc.constraint_schema = rc.constraint_schema
    WHERE tc.table_schema = 'schema_poseidon_tst_16_adm'
      AND tc.table_name   = 'execution'
      AND tc.constraint_name = 'execution_flow_reference_fkey'
  ) THEN
    EXECUTE 'ALTER TABLE schema_poseidon_tst_16_adm.execution
             ADD CONSTRAINT execution_flow_reference_fkey
             FOREIGN KEY (flow_reference)
             REFERENCES schema_poseidon_tst_16_adm.flow_definition_live(reference)';
  END IF;
END$$;

-- 3) execution.output_reference -> output_definition(reference)  (missing in current schema)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_schema='schema_poseidon_tst_16_adm'
      AND table_name='execution'
      AND constraint_name='execution_output_reference_fkey'
  ) THEN
    EXECUTE 'ALTER TABLE schema_poseidon_tst_16_adm.execution
             ADD CONSTRAINT execution_output_reference_fkey
             FOREIGN KEY (output_reference)
             REFERENCES schema_poseidon_tst_16_adm.output_definition(reference)';
  END IF;
END$$;

-- 4) invoice_line.output_reference -> output_definition(reference)  (missing in current schema)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_schema='schema_poseidon_tst_16_adm'
      AND table_name='invoice_line'
      AND constraint_name='invoice_line_output_reference_fkey'
  ) THEN
    EXECUTE 'ALTER TABLE schema_poseidon_tst_16_adm.invoice_line
             ADD CONSTRAINT invoice_line_output_reference_fkey
             FOREIGN KEY (output_reference)
             REFERENCES schema_poseidon_tst_16_adm.output_definition(reference)';
  END IF;
END$$;

-- (Optional) helpful indexes on FK columns (no-ops if they already exist)
CREATE INDEX IF NOT EXISTS invoice_line_flow_reference_idx
  ON schema_poseidon_tst_16_adm.invoice_line(flow_reference);

CREATE INDEX IF NOT EXISTS execution_flow_reference_idx
  ON schema_poseidon_tst_16_adm.execution(flow_reference);

CREATE INDEX IF NOT EXISTS invoice_line_output_reference_idx
  ON schema_poseidon_tst_16_adm.invoice_line(output_reference);

CREATE INDEX IF NOT EXISTS execution_output_reference_idx
  ON schema_poseidon_tst_16_adm.execution(output_reference);

COMMIT;