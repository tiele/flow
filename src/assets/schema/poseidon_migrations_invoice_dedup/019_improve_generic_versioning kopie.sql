-- ===========================================
-- Poseidon SCD2 Hardening (single-table model)
-- - Adds generated valid_range
-- - Adds partial-unique "current" per key
-- - Adds GiST exclusion constraints (no overlaps)
-- - Provides the missing generic_versioning_fn used by existing triggers
-- - Cleans up any overlaps/multiple-currents conservatively
-- ===========================================

BEGIN;

-- 0) Prereq for exclusion constraints mixing equality & ranges
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- 1) Add generated "valid_range" to all five tables (idempotent)
DO $$
DECLARE
  t text;
  col_exists boolean;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'flow_definition',
    'rule_definition',
    'customer_definition',
    'cost_node_definition',
    'cost_allocation_definition'
  ] LOOP
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema='schema_poseidon_tst_16_adm'
        AND table_name=t
        AND column_name='valid_range'
    ) INTO col_exists;

    IF NOT col_exists THEN
      EXECUTE format($SQL$
        ALTER TABLE schema_poseidon_tst_16_adm.%I
        ADD COLUMN valid_range tsrange
          GENERATED ALWAYS AS (
            tsrange(valid_from, COALESCE(valid_to, 'infinity'::timestamp), '[)')
          ) STORED
      $SQL$, t);
    END IF;

    -- 1b) Check: valid_to > valid_from (only if not already there)
    PERFORM 1 FROM pg_constraint c
      JOIN pg_class r ON r.oid=c.conrelid
      JOIN pg_namespace n ON n.oid=r.relnamespace
     WHERE n.nspname='schema_poseidon_tst_16_adm'
       AND r.relname=t
       AND c.conname = t || '_valid_window_check';

    IF NOT FOUND THEN
      EXECUTE format($SQL$
        ALTER TABLE schema_poseidon_tst_16_adm.%I
        ADD CONSTRAINT %I CHECK (
          valid_to IS NULL OR valid_to > valid_from
        )
      $SQL$, t, t || '_valid_window_check');
    END IF;
  END LOOP;
END $$;

-- 2) Ensure partial-unique "current per base/composite" (idempotent)
--    (We KEEP your existing unique(reference).)
--    Normalize where needed: flow, rule, cost_node, customer: by base_reference.
--    cost_allocation: by (cost_node_reference, cost_component_reference).
DO $$
DECLARE
  t text;
  idx text;
  exists_bool boolean;
BEGIN
  -- single-key by base_reference
  FOREACH t IN ARRAY ARRAY['flow_definition','rule_definition','cost_node_definition','customer_definition'] LOOP
    idx := 'uq_current_' || t || '_per_base';
    SELECT EXISTS (
      SELECT 1
      FROM pg_class i
      JOIN pg_index ix ON ix.indexrelid=i.oid
      JOIN pg_class r ON r.oid=ix.indrelid
      JOIN pg_namespace n ON n.oid=r.relnamespace
      WHERE n.nspname='schema_poseidon_tst_16_adm'
        AND r.relname=t
        AND i.relname=idx
    ) INTO exists_bool;

    IF NOT exists_bool THEN
      EXECUTE format($SQL$
        CREATE UNIQUE INDEX %I
          ON schema_poseidon_tst_16_adm.%I (base_reference)
          WHERE (is_current = true)
      $SQL$, idx, t);
    END IF;
  END LOOP;

  -- composite by (cost_node_reference, cost_component_reference)
  idx := 'uq_current_cost_allocation_per_pair';
  SELECT EXISTS (
    SELECT 1
    FROM pg_class i
    JOIN pg_index ix ON ix.indexrelid=i.oid
    JOIN pg_class r ON r.oid=ix.indrelid
    JOIN pg_namespace n ON n.oid=r.relnamespace
    WHERE n.nspname='schema_poseidon_tst_16_adm'
      AND r.relname='cost_allocation_definition'
      AND i.relname=idx
  ) INTO exists_bool;

  IF NOT exists_bool THEN
    CREATE UNIQUE INDEX uq_current_cost_allocation_per_pair
      ON schema_poseidon_tst_16_adm.cost_allocation_definition (cost_node_reference, cost_component_reference)
      WHERE (is_current = true);
  END IF;
END $$;

-- 3) Exclusion constraints (true no-overlap protection)
--    We attach to VALID_RANGE; equality on the business key; range-overlap (&&) on the window.
--    NOTE: cost_allocation allows NULL keys today; exclusion constraints don't compare NULLs.
--          The constraint still protects non-null pairs. Tighten to NOT NULL later if desired.
DO $$
DECLARE
  exists_bool boolean;
BEGIN
  -- flow_definition
  SELECT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class r ON r.oid=c.conrelid
    JOIN pg_namespace n ON n.oid=r.relnamespace
    WHERE n.nspname='schema_poseidon_tst_16_adm'
      AND r.relname='flow_definition'
      AND c.conname='flow_definition_no_overlap'
  ) INTO exists_bool;
  IF NOT exists_bool THEN
    ALTER TABLE schema_poseidon_tst_16_adm.flow_definition
      ADD CONSTRAINT flow_definition_no_overlap
      EXCLUDE USING gist (
        base_reference WITH =,
        valid_range    WITH &&
      );
  END IF;

  -- rule_definition
  SELECT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class r ON r.oid=c.conrelid
    JOIN pg_namespace n ON n.oid=r.relnamespace
    WHERE n.nspname='schema_poseidon_tst_16_adm'
      AND r.relname='rule_definition'
      AND c.conname='rule_definition_no_overlap'
  ) INTO exists_bool;
  IF NOT exists_bool THEN
    ALTER TABLE schema_poseidon_tst_16_adm.rule_definition
      ADD CONSTRAINT rule_definition_no_overlap
      EXCLUDE USING gist (
        base_reference WITH =,
        valid_range    WITH &&
      );
  END IF;

  -- cost_node_definition
  SELECT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class r ON r.oid=c.conrelid
    JOIN pg_namespace n ON n.oid=r.relnamespace
    WHERE n.nspname='schema_poseidon_tst_16_adm'
      AND r.relname='cost_node_definition'
      AND c.conname='cost_node_definition_no_overlap'
  ) INTO exists_bool;
  IF NOT exists_bool THEN
    ALTER TABLE schema_poseidon_tst_16_adm.cost_node_definition
      ADD CONSTRAINT cost_node_definition_no_overlap
      EXCLUDE USING gist (
        base_reference WITH =,
        valid_range    WITH &&
      );
  END IF;

  -- customer_definition
  SELECT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class r ON r.oid=c.conrelid
    JOIN pg_namespace n ON n.oid=r.relnamespace
    WHERE n.nspname='schema_poseidon_tst_16_adm'
      AND r.relname='customer_definition'
      AND c.conname='customer_definition_no_overlap'
  ) INTO exists_bool;
  IF NOT exists_bool THEN
    ALTER TABLE schema_poseidon_tst_16_adm.customer_definition
      ADD CONSTRAINT customer_definition_no_overlap
      EXCLUDE USING gist (
        base_reference WITH =,
        valid_range    WITH &&
      );
  END IF;

  -- cost_allocation_definition (composite)
  SELECT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class r ON r.oid=c.conrelid
    JOIN pg_namespace n ON n.oid=r.relnamespace
    WHERE n.nspname='schema_poseidon_tst_16_adm'
      AND r.relname='cost_allocation_definition'
      AND c.conname='cost_allocation_definition_no_overlap'
  ) INTO exists_bool;
  IF NOT exists_bool THEN
    ALTER TABLE schema_poseidon_tst_16_adm.cost_allocation_definition
      ADD CONSTRAINT cost_allocation_definition_no_overlap
      EXCLUDE USING gist (
        cost_node_reference      WITH =,
        cost_component_reference WITH =,
        valid_range              WITH &&
      );
  END IF;
END $$;

-- 4) Data cleanup (conservative): trim overlaps and normalize "current"
--    Rule: for each key, order by valid_from ASC:
--      - set each row's valid_to to the next row's valid_from if needed
--      - mark current = (valid_to IS NULL AND valid_from <= now())
--    (Run per table; idempotent)
DO $$
DECLARE
  now_ts timestamp := now()::timestamp;
BEGIN
  -- single-key tables
  PERFORM 1;  -- noop to keep DO well-formed

  -- flow_definition
  WITH nx AS (
    SELECT id, base_reference,
           valid_from, valid_to,
           LEAD(valid_from) OVER (PARTITION BY base_reference ORDER BY valid_from) AS next_from
    FROM schema_poseidon_tst_16_adm.flow_definition
  )
  UPDATE schema_poseidon_tst_16_adm.flow_definition t
     SET valid_to = nx.next_from
  FROM nx
  WHERE nx.id = t.id
    AND t.valid_to IS NULL
    AND nx.next_from IS NOT NULL
    AND nx.next_from > t.valid_from;

  UPDATE schema_poseidon_tst_16_adm.flow_definition
     SET is_current = (valid_to IS NULL AND valid_from <= now_ts);

  -- rule_definition
  WITH nx AS (
    SELECT id, base_reference,
           valid_from, valid_to,
           LEAD(valid_from) OVER (PARTITION BY base_reference ORDER BY valid_from) AS next_from
    FROM schema_poseidon_tst_16_adm.rule_definition
  )
  UPDATE schema_poseidon_tst_16_adm.rule_definition t
     SET valid_to = nx.next_from
  FROM nx
  WHERE nx.id = t.id
    AND t.valid_to IS NULL
    AND nx.next_from IS NOT NULL
    AND nx.next_from > t.valid_from;

  UPDATE schema_poseidon_tst_16_adm.rule_definition
     SET is_current = (valid_to IS NULL AND valid_from <= now_ts);

  -- cost_node_definition
  WITH nx AS (
    SELECT id, base_reference,
           valid_from, valid_to,
           LEAD(valid_from) OVER (PARTITION BY base_reference ORDER BY valid_from) AS next_from
    FROM schema_poseidon_tst_16_adm.cost_node_definition
  )
  UPDATE schema_poseidon_tst_16_adm.cost_node_definition t
     SET valid_to = nx.next_from
  FROM nx
  WHERE nx.id = t.id
    AND t.valid_to IS NULL
    AND nx.next_from IS NOT NULL
    AND nx.next_from > t.valid_from;

  UPDATE schema_poseidon_tst_16_adm.cost_node_definition
     SET is_current = (valid_to IS NULL AND valid_from <= now_ts);

  -- customer_definition
  WITH nx AS (
    SELECT id, base_reference,
           valid_from, valid_to,
           LEAD(valid_from) OVER (PARTITION BY base_reference ORDER BY valid_from) AS next_from
    FROM schema_poseidon_tst_16_adm.customer_definition
  )
  UPDATE schema_poseidon_tst_16_adm.customer_definition t
     SET valid_to = nx.next_from
  FROM nx
  WHERE nx.id = t.id
    AND t.valid_to IS NULL
    AND nx.next_from IS NOT NULL
    AND nx.next_from > t.valid_from;

  UPDATE schema_poseidon_tst_16_adm.customer_definition
     SET is_current = (valid_to IS NULL AND valid_from <= now_ts);

  -- cost_allocation_definition (composite)
  WITH nx AS (
    SELECT id, cost_node_reference, cost_component_reference,
           valid_from, valid_to,
           LEAD(valid_from) OVER (
             PARTITION BY cost_node_reference, cost_component_reference
             ORDER BY valid_from
           ) AS next_from
    FROM schema_poseidon_tst_16_adm.cost_allocation_definition
  )
  UPDATE schema_poseidon_tst_16_adm.cost_allocation_definition t
     SET valid_to = nx.next_from
  FROM nx
  WHERE nx.id = t.id
    AND t.valid_to IS NULL
    AND nx.next_from IS NOT NULL
    AND nx.next_from > t.valid_from;

  UPDATE schema_poseidon_tst_16_adm.cost_allocation_definition
     SET is_current = (valid_to IS NULL AND valid_from <= now_ts);
END $$;

-- 5) Provide the missing generic_versioning_fn used by existing triggers
--    (Your schema already created triggers that EXECUTE FUNCTION ...generic_versioning_fn('reference') etc.)
--    Behavior:
--      * If single-key ('reference'): auto-suffix NEW.reference if no -V#### present
--      * If future-dated: insert as scheduled (is_current=false)
--      * If current/past: close the open slice for that key (valid_to := NEW.valid_from, is_current=false), then NEW.is_current=true
CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_fn()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  now_ts     timestamp := now()::timestamp;
  is_future  boolean;
  has_suffix boolean;
  base_ref   text;
  sfx        int;
  key1 text; key2 text;
  k1val text; k2val text;
BEGIN
  IF NEW.valid_from IS NULL THEN
    NEW.valid_from := now_ts;
  END IF;

  is_future := NEW.valid_from > now_ts;

  IF TG_NARGS = 1 AND TG_ARGV[0] = 'reference' THEN
    -- single-key by reference/base_reference
    IF NEW.reference IS NULL THEN
      RAISE EXCEPTION 'reference must be provided (base or suffixed)';
    END IF;

    base_ref   := regexp_replace(NEW.reference, '-V[0-9]{4}$', '');
    has_suffix := NEW.reference ~ '-V[0-9]{4}$';

    IF NOT has_suffix THEN
      -- compute next suffix within this table
      EXECUTE format(
        'SELECT COALESCE(MAX( (regexp_match(reference, ''-V([0-9]{4})$''))[1]::int ), 0)
         FROM %I WHERE base_reference = $1',
        TG_TABLE_NAME
      )
      INTO sfx
      USING base_ref;

      NEW.reference := base_ref || '-V' || lpad((sfx+1)::text, 4, '0');
      -- base_reference is GENERATED ALWAYS; it will compute automatically
    END IF;

    IF is_future THEN
      NEW.is_current := false;
      NEW.valid_to   := NULL;
      RETURN NEW;
    END IF;

    -- close the currently open slice (if any) before we insert NEW
    EXECUTE format(
      'UPDATE %I
         SET valid_to = $1, is_current = false
       WHERE base_reference = $2
         AND is_current = true
         AND valid_to IS NULL
         AND valid_from < $1',
      TG_TABLE_NAME
    )
    USING NEW.valid_from, base_ref;

    NEW.is_current := true;
    NEW.valid_to   := NULL;
    RETURN NEW;

  ELSIF TG_NARGS = 2 THEN
    -- composite-key (e.g., cost_allocation_definition): (key1, key2)
    key1 := TG_ARGV[0]; key2 := TG_ARGV[1];

    EXECUTE format('SELECT ($1).%I::text', key1) INTO k1val USING NEW;
    EXECUTE format('SELECT ($1).%I::text', key2) INTO k2val USING NEW;

    IF k1val IS NULL OR k2val IS NULL THEN
      -- Keep permissive, matching current schema (NULLs allowed). You can tighten later.
      IF is_future THEN
        NEW.is_current := false;
      ELSE
        NEW.is_current := (NEW.valid_to IS NULL);
      END IF;
      RETURN NEW;
    END IF;

    IF is_future THEN
      NEW.is_current := false;
      NEW.valid_to   := NULL;
      RETURN NEW;
    END IF;

    EXECUTE format(
      'UPDATE %I
         SET valid_to = $1, is_current = false
       WHERE %I = $2 AND %I = $3
         AND is_current = true
         AND valid_to IS NULL
         AND valid_from < $1',
      TG_TABLE_NAME, key1, key2
    )
    USING NEW.valid_from, k1val, k2val;

    NEW.is_current := true;
    NEW.valid_to   := NULL;
    RETURN NEW;

  ELSE
    -- passthrough if misconfigured
    RETURN NEW;
  END IF;
END;
$$;

-- (Optional) convenience CURRENT views (read-only helpers; no behavior change)
CREATE OR REPLACE VIEW schema_poseidon_tst_16_adm.flow_definition_current AS
  SELECT * FROM schema_poseidon_tst_16_adm.flow_definition WHERE is_current = true;

CREATE OR REPLACE VIEW schema_poseidon_tst_16_adm.rule_definition_current AS
  SELECT * FROM schema_poseidon_tst_16_adm.rule_definition WHERE is_current = true;

CREATE OR REPLACE VIEW schema_poseidon_tst_16_adm.customer_definition_current AS
  SELECT * FROM schema_poseidon_tst_16_adm.customer_definition WHERE is_current = true;

CREATE OR REPLACE VIEW schema_poseidon_tst_16_adm.cost_node_definition_current AS
  SELECT * FROM schema_poseidon_tst_16_adm.cost_node_definition WHERE is_current = true;

CREATE OR REPLACE VIEW schema_poseidon_tst_16_adm.cost_allocation_definition_current AS
  SELECT * FROM schema_poseidon_tst_16_adm.cost_allocation_definition WHERE is_current = true;

COMMIT;

-- ===========================================
-- Acceptance checks (run manually)
-- ===========================================
-- Overlaps (should return 0 rows each)
-- SELECT base_reference, COUNT(*) FROM flow_definition
--  GROUP BY base_reference
--  HAVING COUNT(*) FILTER (WHERE valid_range && LAG(valid_range) OVER (PARTITION BY base_reference ORDER BY valid_from)) > 0;

-- "At most one current" invariant (should return 0 rows each)
-- SELECT base_reference
-- FROM flow_definition WHERE is_current
-- GROUP BY base_reference HAVING COUNT(*) > 1;

-- INSERT sanity (try locally)
-- INSERT INTO flow_definition(reference, valid_from, content, source_type)
-- VALUES ('FLOW_XYZ', now(), '{}'::jsonb, '...'); -- auto-gets FLOW_XYZ-V0001, flips current