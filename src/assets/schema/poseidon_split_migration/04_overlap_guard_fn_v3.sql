-- 04_v3) Overlap guard function (fix default-param rule + range-type aware)
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:25:50.298120
BEGIN;
CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm._assert_no_overlap(
  p_tbl text,
  p_key1 text,
  p_key2 text DEFAULT NULL,
  p_val1 text DEFAULT NULL,
  p_val2 text DEFAULT NULL,
  p_from text DEFAULT NULL,
  p_to   text DEFAULT NULL,
  p_exclude_id int DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  vf_type    text;
  vt_type    text;
  use_tstz   boolean;
  range_expr text;       -- $1/$2 = (from,to)
  sql_txt    text;
  cnt        int;

  live_schema text := 'schema_poseidon_tst_16_adm';
  hist_reg    regclass;
  hist_tbl    text;
  base_tbl    text;
  cand1       text;
  cand2       text;
BEGIN
  -- Detect base table types
  SELECT data_type INTO vf_type
  FROM information_schema.columns
  WHERE table_schema = live_schema
    AND table_name   = p_tbl
    AND column_name  = 'valid_from';

  SELECT data_type INTO vt_type
  FROM information_schema.columns
  WHERE table_schema = live_schema
    AND table_name   = p_tbl
    AND column_name  = 'valid_to';

  use_tstz := NOT (vf_type = 'timestamp without time zone'
                   AND (vt_type = 'timestamp without time zone' OR vt_type IS NULL));

  IF use_tstz THEN
    range_expr := 'tstzrange($1::timestamptz, COALESCE($2::timestamptz, ''infinity''::timestamptz), ''[)'')';
  ELSE
    range_expr := 'tsrange($1::timestamp, COALESCE($2::timestamp, ''infinity''::timestamp), ''[)'')';
  END IF;

  -- -------- live table check --------
  IF p_key2 IS NULL THEN
    sql_txt := format(
      'SELECT count(*) FROM %I.%I
         WHERE %I = $3
           AND valid_range && %s
           AND ($4 IS NULL OR id <> $4)',
      live_schema, p_tbl, p_key1, range_expr
    );
    EXECUTE sql_txt INTO cnt USING p_from, p_to, p_val1, p_exclude_id;
  ELSE
    sql_txt := format(
      'SELECT count(*) FROM %I.%I
         WHERE %I = $3 AND %I = $4
           AND valid_range && %s
           AND ($5 IS NULL OR id <> $5)',
      live_schema, p_tbl, p_key1, p_key2, range_expr
    );
    EXECUTE sql_txt INTO cnt USING p_from, p_to, p_val1, p_val2, p_exclude_id;
  END IF;

  IF cnt > 0 THEN
    RAISE EXCEPTION 'Temporal overlap in %, key=%', p_tbl, p_val1;
  END IF;

  -- -------- resolve history table name --------
  -- cand1: p_tbl || '_history'  (e.g., flow_definition_live_history)
  cand1 := p_tbl || '_history';

  -- cand2: replace trailing '_live' with '_history'  (e.g., flow_definition_history)
  base_tbl := regexp_replace(p_tbl, '_live$', '', '');  -- only if it ends with _live
  cand2 := base_tbl || '_history';

  -- try cand1 then cand2
  SELECT to_regclass(live_schema || '.' || quote_ident(cand1)) INTO hist_reg;
  IF hist_reg IS NOT NULL THEN
    hist_tbl := cand1;
  ELSE
    SELECT to_regclass(live_schema || '.' || quote_ident(cand2)) INTO hist_reg;
    IF hist_reg IS NOT NULL THEN
      hist_tbl := cand2;
    ELSE
      -- No history table found; skip history check gracefully
      hist_tbl := NULL;
    END IF;
  END IF;

  -- -------- history table check (if present) --------
  IF hist_tbl IS NOT NULL THEN
    IF p_key2 IS NULL THEN
      sql_txt := format(
        'SELECT count(*) FROM %I.%I
           WHERE %I = $3
             AND valid_range && %s',
        live_schema, hist_tbl, p_key1, range_expr
      );
      EXECUTE sql_txt INTO cnt USING p_from, p_to, p_val1;
    ELSE
      sql_txt := format(
        'SELECT count(*) FROM %I.%I
           WHERE %I = $3 AND %I = $4
             AND valid_range && %s',
        live_schema, hist_tbl, p_key1, p_key2, range_expr
      );
      EXECUTE sql_txt INTO cnt USING p_from, p_to, p_val1, p_val2;
    END IF;

    IF cnt > 0 THEN
      RAISE EXCEPTION 'Temporal overlap in % (history=%), key=%', p_tbl, hist_tbl, p_val1;
    END IF;
  END IF;
END;
$$;
COMMIT;