-- ===========================================
-- Poseidon ADM schema bootstrap (single-pass)
-- ===========================================

-- Optional: run everything atomically
BEGIN;

-- ----------------------------
-- 1) Schema
-- ----------------------------
-- DROP SCHEMA schema_poseidon_tst_16_adm CASCADE;
CREATE SCHEMA IF NOT EXISTS schema_poseidon_tst_16_adm AUTHORIZATION poseidon_tst_16_adm;

-- (Optional) make sure we always resolve unqualified names inside our schema.
SET search_path = schema_poseidon_tst_16_adm, public;

-- ----------------------------
-- 2) Core tables with no FKs
-- ----------------------------

-- source_definition
-- DROP TABLE schema_poseidon_tst_16_adm.source_definition;
CREATE TABLE schema_poseidon_tst_16_adm.source_definition (
  id          serial       NOT NULL,
  source_type varchar(255) NOT NULL,
  CONSTRAINT source_definition_pkey PRIMARY KEY (id),
  CONSTRAINT source_definition_source_type_key UNIQUE (source_type)
);

-- output_definition
-- DROP TABLE schema_poseidon_tst_16_adm.output_definition;
CREATE TABLE schema_poseidon_tst_16_adm.output_definition (
  id            serial       NOT NULL,
  creation_user varchar(255) NULL,
  creation_date timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  update_user   varchar(255) NULL,
  update_date   timestamp    NULL,
  reference     varchar(255) NOT NULL,
  "type"        varchar(255) NULL,
  closed        bool         NOT NULL DEFAULT false,
  CONSTRAINT output_definition_pkey PRIMARY KEY (id),
  CONSTRAINT output_definition_reference_key UNIQUE (reference)
);
CREATE INDEX idx_output_definition_closed ON schema_poseidon_tst_16_adm.output_definition (closed);
CREATE INDEX idx_output_definition_type   ON schema_poseidon_tst_16_adm.output_definition (type);

-- parameter
-- DROP TABLE schema_poseidon_tst_16_adm."parameter";
CREATE TABLE schema_poseidon_tst_16_adm."parameter" (
  id    serial       NOT NULL,
  pkey  varchar(255) NOT NULL,
  pvalue jsonb       NOT NULL,
  CONSTRAINT parameter_pkey    PRIMARY KEY (id),
  CONSTRAINT parameter_pkey_key UNIQUE (pkey)
);

-- translation
-- DROP TABLE schema_poseidon_tst_16_adm."translation";
CREATE TABLE schema_poseidon_tst_16_adm."translation" (
  id          serial       NOT NULL,
  code        varchar(255) NOT NULL,
  fr_label    varchar(255) NOT NULL,
  nl_label    varchar(255) NOT NULL,
  is_frontend bool         NOT NULL DEFAULT true,
  CONSTRAINT translation_pkey PRIMARY KEY (id),
  CONSTRAINT translation_code_key UNIQUE (code)
);

-- usage_batch (independent)
-- DROP TABLE schema_poseidon_tst_16_adm.usage_batch;
CREATE TABLE schema_poseidon_tst_16_adm.usage_batch (
  id                 serial       NOT NULL,
  creation_user      varchar(255) NULL,
  creation_date      timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  update_user        varchar(255) NULL,
  update_date        timestamp    NULL,
  reference          varchar(255) NOT NULL,
  source_reference   varchar(255) NOT NULL,
  extraction_timestamp timestamp  NULL,
  source_type        varchar(255) NULL,
  CONSTRAINT usage_batch_pkey PRIMARY KEY (id),
  CONSTRAINT usage_batch_reference_key UNIQUE (reference)
);
CREATE INDEX idx_usage_batch_source_ref ON schema_poseidon_tst_16_adm.usage_batch (source_reference);

-- user
-- DROP TABLE schema_poseidon_tst_16_adm."user";
CREATE TABLE schema_poseidon_tst_16_adm."user" (
  id    serial       NOT NULL,
  "oid" varchar(255) NOT NULL,
  "label" varchar(255) NULL,
  CONSTRAINT user_pkey PRIMARY KEY (id),
  CONSTRAINT user_oid_key UNIQUE (oid)
);
CREATE INDEX idx_oid ON schema_poseidon_tst_16_adm."user" (oid);

-- ----------------------------
-- 3) Tables that reference source_definition
-- ----------------------------

-- flow_definition (references source_definition)
-- DROP TABLE schema_poseidon_tst_16_adm.flow_definition;
CREATE TABLE schema_poseidon_tst_16_adm.flow_definition (
  id              serial       NOT NULL,
  creation_user   varchar(255) NULL,
  creation_date   timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  valid_from      timestamp    NOT NULL,
  valid_to        timestamp    NULL,
  is_current      bool         NOT NULL DEFAULT true,
  versioning_comment varchar(4000) NULL,
  reference       varchar(255) NOT NULL,
  source_type     varchar(255) NOT NULL,
  description     varchar(1000) NULL,
  "content"       jsonb        NOT NULL,
  user_comment    varchar(4000) NULL,
  base_reference  varchar(255) GENERATED ALWAYS AS (regexp_replace(reference, '-V[0-9]{4}$', '')) STORED,
  CONSTRAINT flow_definition_pkey PRIMARY KEY (id),
  CONSTRAINT flow_definition_reference_key UNIQUE (reference),
  CONSTRAINT flow_definition_source_type_fkey
    FOREIGN KEY (source_type)
    REFERENCES schema_poseidon_tst_16_adm.source_definition(source_type)
);
CREATE INDEX idx_flow_definition_is_current ON schema_poseidon_tst_16_adm.flow_definition (is_current);
CREATE INDEX idx_flow_definition_source_type ON schema_poseidon_tst_16_adm.flow_definition (source_type);
CREATE UNIQUE INDEX uniq_current_flow_per_reference
  ON schema_poseidon_tst_16_adm.flow_definition (reference) WHERE (is_current = true);

-- execution (references flow_definition, source_definition)
-- DROP TABLE schema_poseidon_tst_16_adm.execution;
CREATE TABLE schema_poseidon_tst_16_adm.execution (
  id                serial       NOT NULL,
  creation_user     varchar(255) NULL,
  creation_date     timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  batch_reference   varchar(255) NULL,
  usage_reference   varchar(255) NULL,
  source_type       varchar(255) NULL,
  flow_reference    varchar(255) NULL,
  output_reference  varchar(255) NULL,
  number_to_treat   int4         NULL,
  number_treated    int4         NULL,
  number_ko         int4         NULL,
  status            varchar(255) NULL,
  processing_message jsonb       NULL,
  "data"            jsonb        NULL,
  CONSTRAINT execution_pkey PRIMARY KEY (id),
  CONSTRAINT execution_flow_reference_fkey
    FOREIGN KEY (flow_reference)
    REFERENCES schema_poseidon_tst_16_adm.flow_definition(reference),
  CONSTRAINT execution_source_type_fkey
    FOREIGN KEY (source_type)
    REFERENCES schema_poseidon_tst_16_adm.source_definition(source_type)
);
CREATE INDEX idx_execution_status ON schema_poseidon_tst_16_adm.execution (status);

-- ----------------------------
-- 4) Tables that reference usage_batch and/or others
-- ----------------------------

-- usage (references usage_batch)
-- DROP TABLE schema_poseidon_tst_16_adm."usage";
CREATE TABLE schema_poseidon_tst_16_adm."usage" (
  id                      serial       NOT NULL,
  creation_user           varchar(255) NULL,
  creation_date           timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  reference               varchar(255) NOT NULL,
  batch_reference         varchar(255) NOT NULL,
  business_timestamp      timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  cost_component_reference varchar(255) NOT NULL,
  "content"               jsonb        NOT NULL,
  CONSTRAINT usage_pkey PRIMARY KEY (id),
  CONSTRAINT usage_reference_key UNIQUE (reference),
  CONSTRAINT usage_batch_reference_fkey
    FOREIGN KEY (batch_reference)
    REFERENCES schema_poseidon_tst_16_adm.usage_batch(reference)
);

-- invoice_line (references flow_definition, source_definition, usage_batch)
-- DROP TABLE schema_poseidon_tst_16_adm.invoice_line;
CREATE TABLE schema_poseidon_tst_16_adm.invoice_line (
  id                      serial       NOT NULL,
  creation_user           varchar(255) NULL,
  creation_date           timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  update_user             varchar(255) NULL,
  update_date             timestamp    NULL,
  reference               varchar(255) NOT NULL,
  batch_reference         varchar(255) NULL,
  source_type             varchar(255) NULL,
  flow_reference          varchar(255) NULL,
  output_reference        varchar(255) NULL,
  provider_org_unit       varchar(255) NULL,
  extraction_timestamp    timestamp    NULL,
  business_timestamp      timestamp    NULL,
  billing_code_provider   varchar(255) NULL,
  billing_code_consumer   varchar(255) NULL,
  production_resource     varchar(255) NULL,
  quantity                float8       NULL,
  unit_cost               float8       NULL,
  total_cost              float8       NULL,
  cost_component_reference varchar(255) NULL,
  cost_component_name     varchar(255) NULL,
  cost_component_type     varchar(255) NULL,
  description             varchar(1024) NULL,
  technical_description   varchar(1024) NULL,
  finance_comment         varchar(4000) NULL,
  environment             varchar(255) NULL,
  anomalies               varchar(255) NULL,
  metadata                jsonb        NULL,
  "function"              varchar(255) NULL,
  invoicing_cycle_id      varchar(255) NULL,
  billing_item_type       varchar(255) NULL,
  line_version_no         int4         NULL,
  is_active               bool         NOT NULL DEFAULT true,
  billing_code_id         varchar(255) NULL,
  billing_code_description varchar(255) NULL,
  customer_id             varchar(255) NULL,
  customer_name           varchar(255) NULL,
  product_name            varchar(255) NULL,
  product_group           varchar(255) NULL,
  product_domain          varchar(255) NULL,
  manual_modified_by_user_id varchar(255) NULL,
  justification           varchar(255) NULL,
  processing_message      jsonb        NULL,
  product_id              varchar(255) NULL,
  product_reference       varchar(255) NULL,
  valid_from              timestamp    NOT NULL,
  valid_to                timestamp    NULL,
  is_current              bool         NOT NULL DEFAULT true,
  versioning_comment      varchar(4000) NULL,
  CONSTRAINT invoice_line_pkey PRIMARY KEY (id),
  CONSTRAINT invoice_line_reference_key UNIQUE (reference),
  CONSTRAINT invoice_line_flow_reference_fkey
    FOREIGN KEY (flow_reference)
    REFERENCES schema_poseidon_tst_16_adm.flow_definition(reference),
  CONSTRAINT invoice_line_source_type_fkey
    FOREIGN KEY (source_type)
    REFERENCES schema_poseidon_tst_16_adm.source_definition(source_type),
  CONSTRAINT invoice_line_usage_batch_reference_fkey
    FOREIGN KEY (batch_reference)
    REFERENCES schema_poseidon_tst_16_adm.usage_batch(reference)
);

-- ----------------------------
-- 5) Remaining domain tables (with versioning)
-- ----------------------------

-- cost_node_definition
-- DROP TABLE schema_poseidon_tst_16_adm.cost_node_definition;
CREATE TABLE schema_poseidon_tst_16_adm.cost_node_definition (
  id                      serial       NOT NULL,
  creation_user           varchar(255) NULL,
  creation_date           timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  valid_from              timestamp    NOT NULL,
  valid_to                timestamp    NULL,
  is_current              bool         NOT NULL DEFAULT true,
  versioning_comment      varchar(4000) NULL,
  reference               varchar(255) NOT NULL,
  parent_node_reference   varchar(255) NULL,
  root_node_reference     varchar(255) NULL,
  "name"                  varchar(255) NULL,
  billing_code_consumer   varchar(255) NULL,
  customer_id             varchar(255) NULL,
  tenant_reference        varchar(255) NULL,
  account_manager_reference varchar(255) NULL,
  service_manager_reference varchar(255) NULL,
  description             varchar(4000) NULL,
  customer_name           varchar(255) NULL,
  base_reference          varchar(255) GENERATED ALWAYS AS (regexp_replace(reference, '-V[0-9]{4}$', '')) STORED,
  CONSTRAINT cost_node_definition_pkey PRIMARY KEY (id),
  CONSTRAINT cost_node_definition_reference_key UNIQUE (reference)
);
CREATE INDEX idx_cost_node_definition_is_current ON schema_poseidon_tst_16_adm.cost_node_definition (is_current);
CREATE INDEX idx_customer_tenant_account_service_current
  ON schema_poseidon_tst_16_adm.cost_node_definition (customer_id, tenant_reference, account_manager_reference, service_manager_reference, is_current);
CREATE UNIQUE INDEX uniq_current_cost_node_reference
  ON schema_poseidon_tst_16_adm.cost_node_definition (reference) WHERE (is_current = true);

-- customer_definition
-- DROP TABLE schema_poseidon_tst_16_adm.customer_definition;
CREATE TABLE schema_poseidon_tst_16_adm.customer_definition (
  id               serial       NOT NULL,
  creation_user    varchar(255) NULL,
  creation_date    timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  valid_from       timestamp    NOT NULL,
  valid_to         timestamp    NULL,
  is_current       bool         NOT NULL DEFAULT true,
  versioning_comment varchar(4000) NULL,
  user_comment     varchar(4000) NULL,
  reference        varchar(255) NOT NULL,
  "content"        jsonb        NOT NULL,
  base_reference   varchar(255) GENERATED ALWAYS AS (regexp_replace(reference, '-V[0-9]{4}$', '')) STORED,
  CONSTRAINT customer_definition_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_customer_definition_is_current ON schema_poseidon_tst_16_adm.customer_definition (is_current);
CREATE UNIQUE INDEX uniq_current_customer_per_reference
  ON schema_poseidon_tst_16_adm.customer_definition (reference) WHERE (is_current = true);

-- rule_definition
-- DROP TABLE schema_poseidon_tst_16_adm.rule_definition;
CREATE TABLE schema_poseidon_tst_16_adm.rule_definition (
  id               serial       NOT NULL,
  creation_user    varchar(255) NULL,
  creation_date    timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  valid_from       timestamp    NOT NULL,
  valid_to         timestamp    NULL,
  is_current       bool         NOT NULL DEFAULT true,
  versioning_comment varchar(4000) NULL,
  user_comment     varchar(4000) NULL,
  reference        varchar(255) NOT NULL,
  "type"           varchar(255) NULL,
  category         varchar(255) NULL,
  description      varchar(1000) NULL,
  "content"        jsonb        NOT NULL,
  base_reference   varchar(255) GENERATED ALWAYS AS (regexp_replace(reference, '-V[0-9]{4}$', '')) STORED,
  CONSTRAINT rule_definition_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_rule_definition_category   ON schema_poseidon_tst_16_adm.rule_definition (category);
CREATE INDEX idx_rule_definition_is_current ON schema_poseidon_tst_16_adm.rule_definition (is_current);
CREATE INDEX idx_rule_definition_type       ON schema_poseidon_tst_16_adm.rule_definition (type);
CREATE UNIQUE INDEX uniq_current_rule_per_base_reference
  ON schema_poseidon_tst_16_adm.rule_definition (base_reference) WHERE (is_current = true);
CREATE UNIQUE INDEX uniq_current_rule_per_reference
  ON schema_poseidon_tst_16_adm.rule_definition (reference) WHERE (is_current = true);

-- cost_allocation_definition
-- DROP TABLE schema_poseidon_tst_16_adm.cost_allocation_definition;
CREATE TABLE schema_poseidon_tst_16_adm.cost_allocation_definition (
  id                           serial       NOT NULL,
  creation_user                varchar(255) NULL,
  creation_date                timestamptz  NULL DEFAULT CURRENT_TIMESTAMP,
  valid_from                   timestamp    NOT NULL,
  valid_to                     timestamp    NULL,
  is_current                   bool         NOT NULL DEFAULT true,
  versioning_comment           varchar(4000) NULL,
  cost_node_reference          varchar(255) NULL,
  cost_component_reference     varchar(255) NULL,
  cost_component_name          varchar(255) NULL,
  source_type                  varchar(255) NULL,
  status                       varchar(255) NULL,
  description                  varchar(4000) NULL,
  cost_component_type          varchar(255) NULL,
  cost_component_status        varchar(255) NULL,
  cost_component_function      varchar(255) NULL,
  cost_component_environment   varchar(255) NULL,
  anomalies                    varchar(255) NULL,
  CONSTRAINT cost_allocation_definition_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_cost_allocation_definition_is_current
  ON schema_poseidon_tst_16_adm.cost_allocation_definition (is_current);
CREATE INDEX idx_cost_allocation_definition_source_ref
  ON schema_poseidon_tst_16_adm.cost_allocation_definition (source_type);
CREATE UNIQUE INDEX uniq_current_component_per_node
  ON schema_poseidon_tst_16_adm.cost_allocation_definition (cost_node_reference, cost_component_reference)
  WHERE (is_current = true);

-- ----------------------------
-- 6) Functions (before triggers!)
-- ----------------------------

-- enforce_cost_node_active (currently not bound to a trigger in your script)
CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm.enforce_cost_node_active()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  IF NEW.cost_node_reference IS NOT NULL THEN
    PERFORM 1
      FROM schema_poseidon_tst_16_adm.cost_node_definition nd
     WHERE nd.reference   = NEW.cost_node_reference
       AND nd.is_current  = true
       AND nd.valid_from <= NEW.valid_from
       AND (nd.valid_to   IS NULL OR nd.valid_to > NEW.valid_from);

    IF NOT FOUND THEN
      RAISE EXCEPTION
        'Invalid cost_node_reference "%": no active version at %',
        NEW.cost_node_reference, NEW.valid_from;
    END IF;
  END IF;
  RETURN NEW;
END;
$function$;

-- generic_versioning_fn
CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_fn()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
  p_key1            TEXT := TG_ARGV[0];
  p_key2            TEXT := COALESCE(TG_ARGV[1], '');
  val1              TEXT;
  val2              TEXT;
  base_ref          TEXT;
  like_pattern      TEXT;
  max_vers          INT;
  has_update_user   BOOLEAN;
  full_ref          TEXT;
  update_sql        TEXT;
BEGIN
  -- Special-case cost_allocation_definition (no suffix bump, retire by pair)
  IF TG_TABLE_NAME = 'cost_allocation_definition' THEN
    SELECT EXISTS(
      SELECT 1
        FROM information_schema.columns
       WHERE table_schema = TG_TABLE_SCHEMA
         AND table_name   = TG_TABLE_NAME
         AND column_name  = 'update_user'
    ) INTO has_update_user;

    update_sql := format(
      'UPDATE %I.%I
          SET is_current = false,
              valid_to    = now()%s
        WHERE is_current = true
          AND cost_node_reference      = $1
          AND cost_component_reference = $2',
      TG_TABLE_SCHEMA, TG_TABLE_NAME,
      CASE WHEN has_update_user THEN ', update_user = $3' ELSE '' END
    );

    IF has_update_user THEN
      EXECUTE update_sql
        USING NEW.cost_node_reference,
              NEW.cost_component_reference,
              NEW.creation_user;
    ELSE
      EXECUTE update_sql
        USING NEW.cost_node_reference,
              NEW.cost_component_reference;
    END IF;

    NEW.valid_from := now();
    NEW.valid_to   := NULL;
    NEW.is_current := true;
    RETURN NEW;
  END IF;

  -- Generic “-V####” logic
  val1 := to_jsonb(NEW) ->> p_key1;
  IF p_key2 <> '' THEN
    val2 := to_jsonb(NEW) ->> p_key2;
  END IF;

  base_ref     := regexp_replace(val1, '-V[0-9]{4}$', '');
  like_pattern := base_ref || '-V____';

  EXECUTE format(
    'SELECT COALESCE(MAX((substring(%1$I FROM ''-V([0-9]{4})$''))::int),0)
       FROM %2$I.%3$I
      WHERE %1$I LIKE %4$L',
    p_key1, TG_TABLE_SCHEMA, TG_TABLE_NAME, like_pattern
  ) INTO max_vers;

  full_ref := base_ref || '-V' || lpad((max_vers + 1)::text, 4, '0');

  IF p_key1 = 'cost_node_reference' THEN
    NEW.cost_node_reference := full_ref;
  ELSIF p_key1 = 'cost_component_reference' THEN
    NEW.cost_component_reference := full_ref;
  ELSIF p_key1 = 'reference' THEN
    NEW.reference := full_ref;
  ELSE
    RAISE EXCEPTION 'generic_versioning_fn: unsupported column "%"', p_key1;
  END IF;

  SELECT EXISTS(
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = TG_TABLE_SCHEMA
       AND table_name   = TG_TABLE_NAME
       AND column_name  = 'update_user'
  ) INTO has_update_user;

  update_sql := format(
    'UPDATE %I.%I
        SET is_current = false,
            valid_to    = now()%s
      WHERE is_current = true
        AND %I LIKE $1%s',
    TG_TABLE_SCHEMA,
    TG_TABLE_NAME,
    CASE WHEN has_update_user THEN ', update_user = $3' ELSE '' END,
    p_key1,
    CASE WHEN p_key2 <> '' THEN ' AND ' || p_key2 || ' = $2' ELSE '' END
  );

  IF p_key2 <> '' THEN
    IF has_update_user THEN
      EXECUTE update_sql USING like_pattern, val2, NEW.creation_user;
    ELSE
      EXECUTE update_sql USING like_pattern, val2;
    END IF;
  ELSE
    IF has_update_user THEN
      EXECUTE update_sql USING like_pattern, NEW.creation_user;
    ELSE
      EXECUTE update_sql USING like_pattern;
    END IF;
  END IF;

  NEW.valid_from := now();
  NEW.valid_to   := NULL;
  NEW.is_current := true;
  RETURN NEW;
END;
$function$;

-- test_new_field (utility)
CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm.test_new_field()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  NEW.valid_from := now();
  RETURN NEW;
END;
$function$;

-- ----------------------------
-- 7) Triggers (after functions)
-- ----------------------------

-- cost_allocation_definition versioning
-- DROP TRIGGER trg_cost_allocation_definition_versioning ON schema_poseidon_tst_16_adm.cost_allocation_definition;
CREATE TRIGGER trg_cost_allocation_definition_versioning
BEFORE INSERT ON schema_poseidon_tst_16_adm.cost_allocation_definition
FOR EACH ROW
EXECUTE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_fn('cost_node_reference', 'cost_component_reference');

-- cost_node_definition versioning
-- DROP TRIGGER trg_cost_node_definition_versioning ON schema_poseidon_tst_16_adm.cost_node_definition;
CREATE TRIGGER trg_cost_node_definition_versioning
BEFORE INSERT ON schema_poseidon_tst_16_adm.cost_node_definition
FOR EACH ROW
EXECUTE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_fn('reference');

-- customer_definition versioning
-- DROP TRIGGER trg_customer_definition_versioning ON schema_poseidon_tst_16_adm.customer_definition;
CREATE TRIGGER trg_customer_definition_versioning
BEFORE INSERT ON schema_poseidon_tst_16_adm.customer_definition
FOR EACH ROW
EXECUTE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_fn('reference');

-- rule_definition versioning
-- DROP TRIGGER trg_rule_definition_versioning ON schema_poseidon_tst_16_adm.rule_definition;
CREATE TRIGGER trg_rule_definition_versioning
BEFORE INSERT ON schema_poseidon_tst_16_adm.rule_definition
FOR EACH ROW
EXECUTE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_fn('reference');

-- flow_definition versioning
-- DROP TRIGGER trg_flow_definition_versioning ON schema_poseidon_tst_16_adm.flow_definition;
CREATE TRIGGER trg_flow_definition_versioning
BEFORE INSERT ON schema_poseidon_tst_16_adm.flow_definition
FOR EACH ROW
EXECUTE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_fn('reference');

-- (If you later want to enforce active cost node on allocation rows, bind enforce_cost_node_active like:)
-- CREATE TRIGGER trg_cost_alloc_enforce_active_node
-- BEFORE INSERT ON schema_poseidon_tst_16_adm.cost_allocation_definition
-- FOR EACH ROW
-- EXECUTE FUNCTION schema_poseidon_tst_16_adm.enforce_cost_node_active();

-- ----------------------------
-- 8) Custom array types (must come AFTER the table row types exist)
--    Note: Postgres already provides array types (e.g., cost_node_definition[]),
--    but these definitions preserve your explicit named array types.
-- ----------------------------

-- ----------------------------
-- 9) Handy indexes (already created inline above where relevant)
-- ----------------------------

-- ----------------------------
-- 10) Commit
-- ----------------------------
COMMIT;