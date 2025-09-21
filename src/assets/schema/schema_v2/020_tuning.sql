-- ======================================================================
-- 020_hardening_idempotency_fk_ilv_provenance.sql
--
-- Scope:
--   1) Simplify EXECUTION idempotency to a single SUCCESS rule on
--      (flow_reference, input_sig, logic_sig) and EXCLUDE trace runs.
--      → Drops legacy/overlapping indexes to avoid surprise dedupe.
--   2) Race-proof invoice_line_version creation:
--      → Add per-reference advisory lock + tolerant insert (ON CONFLICT).
--   3) Make invoice_line_version provenance first-class:
--      → Add NOT VALID FKs to flow_definition, source_definition,
--        usage_batch (and optional output_definition), plus helpful indexes.
--   4) Bind missing integrity check for cost allocations:
--      → Trigger to enforce cost node is active at valid_from.
--   5) Small safety/perf extras:
--      → Drop default sequence on ILV.id (copied by LIKE), add useful indexes.
--
-- Notes:
--   • Uses CONCURRENTLY for low-lock index ops → keep this file OUT of an explicit transaction.
--   • Validation of NOT VALID FKs is a separate, one-time scan you can run off-peak:
--       ALTER TABLE ... VALIDATE CONSTRAINT ...
-- ======================================================================


/* ----------------------------------------------------------------------
 * 1) EXECUTION idempotency: keep ONE success rule, drop the rest
 *
 * Why:
 *  - We remove "active uniqueness" (QUEUED/RUNNING) entirely to avoid
 *    surprises in scheduling logic.
 *  - We drop the old 2-key success (flow_reference, input_sig) rule,
 *    because it blocks legitimate re-runs when logic changes.
 *  - We keep a single SUCCESS idempotency on (flow_reference, input_sig, logic_sig),
 *    while EXCLUDING trace runs (usage_reference IS NOT NULL), per your policy.
 * Locking:
 *  - Use CONCURRENTLY to minimize blocking on a hot execution table.
 * -------------------------------------------------------------------- */

DROP INDEX CONCURRENTLY IF EXISTS schema_poseidon_tst_16_adm.uq_exec_flow_output_active;
DROP INDEX CONCURRENTLY IF EXISTS schema_poseidon_tst_16_adm.uq_exec_success_by_sig;
DROP INDEX CONCURRENTLY IF EXISTS schema_poseidon_tst_16_adm.uq_exec_success_inputs_logic;

CREATE UNIQUE INDEX CONCURRENTLY uq_exec_success_inputs_logic
  ON schema_poseidon_tst_16_adm.execution (flow_reference, input_sig, logic_sig)
  WHERE status = 'SUCCESS' AND usage_reference IS NULL;


/* ----------------------------------------------------------------------
 * 2) Race-proof invoice_line_version version numbering
 *
 * Problem:
 *  - The ILV trigger computes next version_no as max+1 per (reference).
 *    Two concurrent inserts for the same reference can race → PK conflict.
 *
 * Fix:
 *  - Redefine the AFTER INSERT trigger function to:
 *      a) Take an advisory lock per NEW.reference before computing max+1.
 *      b) Use ON CONFLICT (reference, content_hash) DO NOTHING to be tolerant
 *         if the same content wins the race elsewhere.
 *
 * Notes:
 *  - We preserve your existing logic, only adding:
 *       PERFORM pg_advisory_xact_lock(hashtext(NEW.reference));
 *    just before computing the NEW version number, and
 *       ... ON CONFLICT (reference, content_hash) DO NOTHING
 *    on the insert into invoice_line_version.
 * -------------------------------------------------------------------- */

CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm.trg_invoice_line_after_ins()
RETURNS trigger
LANGUAGE plpgsql
AS $$
declare
  v_closed bool;
  v_ver    int;
begin
  -- ensure hash on base row (for diagnostics)
  if NEW.content_hash is null then
    update schema_poseidon_tst_16_adm.invoice_line
       set content_hash = schema_poseidon_tst_16_adm._hash_invoice_line(NEW)
     where id = NEW.id;
  end if;

  -- destination must be open
  select closed into v_closed
  from   schema_poseidon_tst_16_adm.output_definition
  where  reference = NEW.output_reference;
  if coalesce(v_closed,false) then
    raise exception 'Cannot add/replace lines in closed output %', NEW.output_reference;
  end if;

  -- compute hash for versioning
  NEW.content_hash := schema_poseidon_tst_16_adm._hash_invoice_line(NEW);

  -- identical version already present?
  select line_version_no
    into v_ver
  from schema_poseidon_tst_16_adm.invoice_line_version
  where reference    = NEW.reference
    and content_hash = NEW.content_hash
  order by line_version_no desc
  limit 1;

  if v_ver is null then
    -- Prevent concurrent next-version race for this reference
    PERFORM pg_advisory_xact_lock(hashtext(NEW.reference));

    v_ver := coalesce(
      NEW.line_version_no,
      (select coalesce(max(line_version_no),0)+1
         from schema_poseidon_tst_16_adm.invoice_line_version
        where reference = NEW.reference)
    );

    insert into schema_poseidon_tst_16_adm.invoice_line_version
    (
      id, creation_user, creation_date, update_user, update_date,
      reference, batch_reference, source_type, flow_reference, output_reference,
      provider_org_unit, extraction_timestamp, business_timestamp,
      billing_code_provider, billing_code_consumer, production_resource,
      quantity, unit_cost, total_cost, cost_component_reference,
      cost_component_name, cost_component_type, description, technical_description,
      finance_comment, environment, anomalies, metadata, "function",
      invoicing_cycle_id, billing_item_type, line_version_no, is_active,
      billing_code_id, billing_code_description, customer_id, customer_name,
      product_name, product_group, product_domain, manual_modified_by_user_id,
      justification, processing_message, product_id, product_reference,
      valid_from, valid_to, is_current, versioning_comment, content_hash
    )
    values
    (
      NEW.id, NEW.creation_user, NEW.creation_date, NEW.update_user, NEW.update_date,
      NEW.reference, NEW.batch_reference, NEW.source_type, NEW.flow_reference, NEW.output_reference,
      NEW.provider_org_unit, NEW.extraction_timestamp, NEW.business_timestamp,
      NEW.billing_code_provider, NEW.billing_code_consumer, NEW.production_resource,
      NEW.quantity, NEW.unit_cost, NEW.total_cost, NEW.cost_component_reference,
      NEW.cost_component_name, NEW.cost_component_type, NEW.description, NEW.technical_description,
      NEW.finance_comment, NEW.environment, NEW.anomalies, NEW.metadata, NEW."function",
      NEW.invoicing_cycle_id, NEW.billing_item_type, v_ver, NEW.is_active,
      NEW.billing_code_id, NEW.billing_code_description, NEW.customer_id, NEW.customer_name,
      NEW.product_name, NEW.product_group, NEW.product_domain, NEW.manual_modified_by_user_id,
      NEW.justification, NEW.processing_message, NEW.product_id, NEW.product_reference,
      NEW.valid_from, NEW.valid_to, NEW.is_current, NEW.versioning_comment, NEW.content_hash
    )
    ON CONFLICT (reference, content_hash) DO NOTHING;
  end if;

  -- snapshot membership (upsert to latest version)
  insert into schema_poseidon_tst_16_adm.output_invoice_line (output_reference, line_reference, line_version_no)
  values (NEW.output_reference, NEW.reference, coalesce(v_ver, 1))
  on conflict (output_reference, line_reference)
  do update set line_version_no = excluded.line_version_no;

  return null;
end
$$;

-- (Re-create the trigger binding in case it was dropped elsewhere)
DROP TRIGGER IF EXISTS invoice_line_after_ins ON schema_poseidon_tst_16_adm.invoice_line;
CREATE TRIGGER invoice_line_after_ins
AFTER INSERT ON schema_poseidon_tst_16_adm.invoice_line
FOR EACH ROW EXECUTE FUNCTION schema_poseidon_tst_16_adm.trg_invoice_line_after_ins();


/* ----------------------------------------------------------------------
 * 3) Invoice-line-version provenance: add NOT VALID foreign keys
 *
 * Why:
 *  - ILV is your immutable source of truth for published lines. Adding FKs
 *    anchors each version to the domain it came from (flow/source/batch/...).
 * How:
 *  - Use NOT VALID so the FK begins protecting **new** writes immediately,
 *    without scanning existing data (online-ish DDL).
 *  - You can VALIDATE later, off-peak. See validation block at the end.
 *
 * Indexing:
 *  - Add child-side indexes to make validation quick and parent deletes/updates
 *    efficient. Parent sides already have PK/UNIQUE indexes.
 * -------------------------------------------------------------------- */

-- Helpful child-side indexes (no lock storms)
CREATE INDEX IF NOT EXISTS idx_ilv_flow_ref
  ON schema_poseidon_tst_16_adm.invoice_line_version(flow_reference);
CREATE INDEX IF NOT EXISTS idx_ilv_source_type
  ON schema_poseidon_tst_16_adm.invoice_line_version(source_type);
CREATE INDEX IF NOT EXISTS idx_ilv_batch_ref
  ON schema_poseidon_tst_16_adm.invoice_line_version(batch_reference);
CREATE INDEX IF NOT EXISTS idx_ilv_output_ref
  ON schema_poseidon_tst_16_adm.invoice_line_version(output_reference);

-- Add NOT VALID FKs (immediately enforce on NEW rows; defer full scan)
ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version
  ADD CONSTRAINT fk_ilv_flow    FOREIGN KEY (flow_reference)
    REFERENCES schema_poseidon_tst_16_adm.flow_definition(reference) NOT VALID,
  ADD CONSTRAINT fk_ilv_src     FOREIGN KEY (source_type)
    REFERENCES schema_poseidon_tst_16_adm.source_definition(source_type) NOT VALID,
  ADD CONSTRAINT fk_ilv_batch   FOREIGN KEY (batch_reference)
    REFERENCES schema_poseidon_tst_16_adm.usage_batch(reference) NOT VALID,
  ADD CONSTRAINT fk_ilv_output  FOREIGN KEY (output_reference)
    REFERENCES schema_poseidon_tst_16_adm.output_definition(reference) NOT VALID;

-- Remove the auto-copied sequence default from ILV.id (copied via LIKE INCLUDING ALL)
-- Rationale: ILV.id is not the PK and we explicitly supply NEW.id from writer;
--            keeping a default nextval here is confusing and can cause surprises.
ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version
  ALTER COLUMN id DROP DEFAULT;


 /* ---------------------------------------------------------------------
  * 4) Bind the missing integrity check for cost allocations
  *
  * Why:
  *  - The function enforce_cost_node_active() exists but wasn't attached.
  *    Without it, you can insert allocations pointing to an inactive /
  *    non-covering cost node at valid_from.
  * How:
  *  - BEFORE INSERT OR UPDATE trigger, so both new rows and date changes
  *    are checked.
  * -------------------------------------------------------------------- */

DROP TRIGGER IF EXISTS trg_cost_alloc_enforce_active_node
  ON schema_poseidon_tst_16_adm.cost_allocation_definition;

CREATE TRIGGER trg_cost_alloc_enforce_active_node
BEFORE INSERT OR UPDATE ON schema_poseidon_tst_16_adm.cost_allocation_definition
FOR EACH ROW EXECUTE FUNCTION schema_poseidon_tst_16_adm.enforce_cost_node_active();


/* ----------------------------------------------------------------------
 * 5) Small safety/perf extras
 * -------------------------------------------------------------------- */

-- Speed up usage lookups by batch (used in input_sig aggregation paths)
CREATE INDEX IF NOT EXISTS idx_usage_batch_ref
  ON schema_poseidon_tst_16_adm."usage"(batch_reference);

-- (Optional but generally helpful) execution timeline indexes for dashboards/history
CREATE INDEX IF NOT EXISTS idx_execution_run_at
  ON schema_poseidon_tst_16_adm.execution(run_at);
CREATE INDEX IF NOT EXISTS idx_execution_flow_run_at
  ON schema_poseidon_tst_16_adm.execution(flow_reference, run_at DESC);


/* ----------------------------------------------------------------------
 * 6) (Optional) Off-peak FK validation helpers
 *
 * Run these later in a quiet window. They scan invoice_line_version and
 * verify existing rows; NEW rows have been enforced since the NOT VALID add.
 *
 * You can pre-check for orphans first, e.g.:
 *   SELECT t.*
 *   FROM schema_poseidon_tst_16_adm.invoice_line_version t
 *   LEFT JOIN schema_poseidon_tst_16_adm.usage_batch b
 *     ON b.reference = t.batch_reference
 *   WHERE t.batch_reference IS NOT NULL AND b.reference IS NULL
 *   LIMIT 50;
 * -------------------------------------------------------------------- */

-- ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version VALIDATE CONSTRAINT fk_ilv_flow;
-- ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version VALIDATE CONSTRAINT fk_ilv_src;
-- ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version VALIDATE CONSTRAINT fk_ilv_batch;
-- ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version VALIDATE CONSTRAINT fk_ilv_output;

-- ======================================================================
-- End of migration 020
-- ======================================================================

-- ======================================================================
-- 021_sync_execution_fk_to_ilv.sql
--
-- Goal:
--   Keep provenance aligned by carrying execution_id from writer
--   (invoice_line) into the immutable store (invoice_line_version).
--
-- What this does:
--   1) Add execution_id to invoice_line and invoice_line_version (if missing)
--   2) Add NOT VALID foreign keys to execution(id) on both tables
--   3) Add helpful indexes for execution_id on both tables
--   4) Update the invoice_line AFTER INSERT trigger function to include
--      execution_id in the ILV insert so they stay in sync.
--
-- Design notes:
--   • Using NOT VALID means NEW writes are checked immediately, but we skip
--     scanning existing rows. Later, you can run VALIDATE in a quiet window.
--   • Child-side indexes speed parent deletes/updates and FK validation.
--   • Trigger keeps your existing logic; we only add execution_id pass-through.
-- ======================================================================


/* ----------------------------------------------------------------------
 * 1) Columns: add execution_id where missing.
 *    Use int4 to match execution(id) which is serial (int).
 * -------------------------------------------------------------------- */

ALTER TABLE schema_poseidon_tst_16_adm.invoice_line
  ADD COLUMN IF NOT EXISTS execution_id int4 NULL;

ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version
  ADD COLUMN IF NOT EXISTS execution_id int4 NULL;


/* ----------------------------------------------------------------------
 * 2) Indexes: add child-side indexes for FK efficiency and validation speed.
 *    CONCURRENTLY to avoid long locks on large tables.
 * -------------------------------------------------------------------- */

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoice_line_execution_id
  ON schema_poseidon_tst_16_adm.invoice_line(execution_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ilv_execution_id
  ON schema_poseidon_tst_16_adm.invoice_line_version(execution_id);


/* ----------------------------------------------------------------------
 * 3) FKs: attach to execution(id) as NOT VALID (online-ish).
 *    If the writer FK already exists, we skip adding it.
 * -------------------------------------------------------------------- */

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   information_schema.table_constraints
    WHERE  table_schema = 'schema_poseidon_tst_16_adm'
      AND  table_name   = 'invoice_line'
      AND  constraint_name = 'fk_invoice_line_execution'
      AND  constraint_type = 'FOREIGN KEY'
  ) THEN
    ALTER TABLE schema_poseidon_tst_16_adm.invoice_line
      ADD CONSTRAINT fk_invoice_line_execution
      FOREIGN KEY (execution_id)
      REFERENCES schema_poseidon_tst_16_adm.execution(id)
      NOT VALID;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   information_schema.table_constraints
    WHERE  table_schema = 'schema_poseidon_tst_16_adm'
      AND  table_name   = 'invoice_line_version'
      AND  constraint_name = 'fk_ilv_execution'
      AND  constraint_type = 'FOREIGN KEY'
  ) THEN
    ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version
      ADD CONSTRAINT fk_ilv_execution
      FOREIGN KEY (execution_id)
      REFERENCES schema_poseidon_tst_16_adm.execution(id)
      NOT VALID;
  END IF;
END $$;


/* ----------------------------------------------------------------------
 * 4) Trigger function: ensure execution_id is copied to ILV.
 *
 *    This replaces the function from the last migration so the immutable
 *    version table always carries the same execution_id as its writer row.
 *
 *    Notes:
 *    • We keep the advisory lock to avoid per-reference version number races.
 *    • We keep the ON CONFLICT (reference, content_hash) DO NOTHING to be
 *      tolerant if identical content races in.
 * -------------------------------------------------------------------- */

CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm.trg_invoice_line_after_ins()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_closed bool;
  v_ver    int;
BEGIN
  -- ensure hash on base row (for diagnostics)
  IF NEW.content_hash IS NULL THEN
    UPDATE schema_poseidon_tst_16_adm.invoice_line
       SET content_hash = schema_poseidon_tst_16_adm._hash_invoice_line(NEW)
     WHERE id = NEW.id;
  END IF;

  -- destination must be open
  SELECT closed INTO v_closed
  FROM   schema_poseidon_tst_16_adm.output_definition
  WHERE  reference = NEW.output_reference;
  IF coalesce(v_closed,false) THEN
    RAISE EXCEPTION 'Cannot add/replace lines in closed output %', NEW.output_reference;
  END IF;

  -- compute hash for versioning
  NEW.content_hash := schema_poseidon_tst_16_adm._hash_invoice_line(NEW);

  -- identical version already present?
  SELECT line_version_no
    INTO v_ver
  FROM schema_poseidon_tst_16_adm.invoice_line_version
  WHERE reference    = NEW.reference
    AND content_hash = NEW.content_hash
  ORDER BY line_version_no DESC
  LIMIT 1;

  IF v_ver IS NULL THEN
    -- Prevent concurrent next-version race for this reference
    PERFORM pg_advisory_xact_lock(hashtext(NEW.reference));

    v_ver := coalesce(
      NEW.line_version_no,
      (SELECT coalesce(max(line_version_no),0)+1
         FROM schema_poseidon_tst_16_adm.invoice_line_version
        WHERE reference = NEW.reference)
    );

    INSERT INTO schema_poseidon_tst_16_adm.invoice_line_version
    (
      id, creation_user, creation_date, update_user, update_date,
      reference, batch_reference, source_type, flow_reference, output_reference,
      provider_org_unit, extraction_timestamp, business_timestamp,
      billing_code_provider, billing_code_consumer, production_resource,
      quantity, unit_cost, total_cost, cost_component_reference,
      cost_component_name, cost_component_type, description, technical_description,
      finance_comment, environment, anomalies, metadata, "function",
      invoicing_cycle_id, billing_item_type, line_version_no, is_active,
      billing_code_id, billing_code_description, customer_id, customer_name,
      product_name, product_group, product_domain, manual_modified_by_user_id,
      justification, processing_message, product_id, product_reference,
      valid_from, valid_to, is_current, versioning_comment, content_hash,
      execution_id                         -- ★ NEW: carry execution provenance
    )
    VALUES
    (
      NEW.id, NEW.creation_user, NEW.creation_date, NEW.update_user, NEW.update_date,
      NEW.reference, NEW.batch_reference, NEW.source_type, NEW.flow_reference, NEW.output_reference,
      NEW.provider_org_unit, NEW.extraction_timestamp, NEW.business_timestamp,
      NEW.billing_code_provider, NEW.billing_code_consumer, NEW.production_resource,
      NEW.quantity, NEW.unit_cost, NEW.total_cost, NEW.cost_component_reference,
      NEW.cost_component_name, NEW.cost_component_type, NEW.description, NEW.technical_description,
      NEW.finance_comment, NEW.environment, NEW.anomalies, NEW.metadata, NEW."function",
      NEW.invoicing_cycle_id, NEW.billing_item_type, v_ver, NEW.is_active,
      NEW.billing_code_id, NEW.billing_code_description, NEW.customer_id, NEW.customer_name,
      NEW.product_name, NEW.product_group, NEW.product_domain, NEW.manual_modified_by_user_id,
      NEW.justification, NEW.processing_message, NEW.product_id, NEW.product_reference,
      NEW.valid_from, NEW.valid_to, NEW.is_current, NEW.versioning_comment, NEW.content_hash,
      NEW.execution_id                      -- ★ NEW: pass through from writer
    )
    ON CONFLICT (reference, content_hash) DO NOTHING;
  END IF;

  -- snapshot membership (upsert to latest version)
  INSERT INTO schema_poseidon_tst_16_adm.output_invoice_line (output_reference, line_reference, line_version_no)
  VALUES (NEW.output_reference, NEW.reference, coalesce(v_ver, 1))
  ON CONFLICT (output_reference, line_reference)
  DO UPDATE SET line_version_no = EXCLUDED.line_version_no;

  RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS invoice_line_after_ins ON schema_poseidon_tst_16_adm.invoice_line;
CREATE TRIGGER invoice_line_after_ins
AFTER INSERT ON schema_poseidon_tst_16_adm.invoice_line
FOR EACH ROW EXECUTE FUNCTION schema_poseidon_tst_16_adm.trg_invoice_line_after_ins();


/* ----------------------------------------------------------------------
 * 5) (Optional) VALIDATE off-peak
 *
 *    Existing rows are not scanned by NOT VALID; new rows are enforced now.
 *    Later, run these during a quiet window to fully validate the FKs:
 *
 * -- ALTER TABLE schema_poseidon_tst_16_adm.invoice_line          VALIDATE CONSTRAINT fk_invoice_line_execution;
 * -- ALTER TABLE schema_poseidon_tst_16_adm.invoice_line_version  VALIDATE CONSTRAINT fk_ilv_execution;
 *
 *    You can pre-check for orphans, e.g.:
 *    SELECT il.*
 *    FROM schema_poseidon_tst_16_adm.invoice_line il
 *    LEFT JOIN schema_poseidon_tst_16_adm.execution e ON e.id = il.execution_id
 *    WHERE il.execution_id IS NOT NULL AND e.id IS NULL
 *    LIMIT 50;
 *
 *    SELECT ilv.*
 *    FROM schema_poseidon_tst_16_adm.invoice_line_version ilv
 *    LEFT JOIN schema_poseidon_tst_16_adm.execution e ON e.id = ilv.execution_id
 *    WHERE ilv.execution_id IS NOT NULL AND e.id IS NULL
 *    LIMIT 50;
 * -------------------------------------------------------------------- */

-- ======================================================================
-- End migration 021
-- ======================================================================

CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_fn()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_reg   regclass;
  qtext     text;
  base_ref  text;
  last_ver  int;
  now_ts    timestamptz := current_timestamp;
BEGIN
  -- Use the exact table that fired the trigger
  tbl_reg := format('%I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME)::regclass;

  -- Derive base_reference
  IF NEW.base_reference IS NOT NULL THEN
    base_ref := NEW.base_reference;
  ELSIF NEW.reference IS NOT NULL THEN
    base_ref := regexp_replace(NEW.reference, '-V[0-9]{4}$', '');
  ELSE
    RAISE EXCEPTION 'generic_versioning_fn: NEW.reference (or base_reference) must be provided';
  END IF;

  -- Get last used version for this base_reference from the same table
  qtext := format($q$
    SELECT COALESCE(MAX( (regexp_match(reference, '-V([0-9]{4})$'))[1]::int ), 0)
    FROM %s
    WHERE base_reference = $1
  $q$, tbl_reg);

  EXECUTE qtext INTO last_ver USING base_ref;

  -- Build next reference and normalize SCD2 fields
  NEW.base_reference := base_ref;
  NEW.reference      := base_ref || '-V' || LPAD((last_ver + 1)::text, 4, '0');
  NEW.is_current     := TRUE;
  NEW.valid_from     := COALESCE(NEW.valid_from, now_ts);
  NEW.valid_to       := NULL;

  -- Close the previous current
  qtext := format($q$
    UPDATE %s
       SET is_current = FALSE,
           valid_to   = $2
     WHERE base_reference = $1
       AND is_current = TRUE
  $q$, tbl_reg);

  EXECUTE qtext USING base_ref, NEW.valid_from;

  RETURN NEW;
END
$$;
