-- 05_v5) Versioning trigger (no declared args; reads keys from TG_ARGV)
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:30:18.755815
BEGIN;

CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm._versioning_apply(
  p_schema   text,
  p_live_tbl text,
  p_keys     text[],     -- e.g., ARRAY['reference'] or ARRAY['cost_node_reference','cost_component_reference']
  p_new      jsonb,
  p_now      timestamptz DEFAULT now()
) RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
  v_schema   text := p_schema;
  live_tbl   text := p_live_tbl;

  cand1      text := live_tbl || '_history';
  base_tbl   text := regexp_replace(live_tbl, '_live$', '', '');
  cand2      text := base_tbl || '_history';
  hist_tbl   text;
  r          regclass;

  k1   text := p_keys[1];
  k2   text := NULLIF(p_keys[2], '');
  val1 text := p_new ->> k1;
  val2 text := CASE WHEN k2 IS NULL THEN NULL ELSE p_new ->> k2 END;

  vf_txt text := p_new->>'valid_from';
  vt_txt text := p_new->>'valid_to';

  is_future boolean := COALESCE((p_new->>'valid_from')::timestamp > (p_now::timestamp), false);

  next_ver int;
  col_list_hist text;
  col_list_live text;
BEGIN
  IF k1 IS NULL OR k1 = '' THEN
    RAISE EXCEPTION 'Key[1] must be provided';
  END IF;

  IF val1 ~ '-V[0-9]{4}$' THEN
    RAISE EXCEPTION 'Live key value % should not include -V#### suffix', val1;
  END IF;

  -- Ensure valid_from exists (as plain timestamp)
  IF vf_txt IS NULL OR vf_txt = '' THEN
    p_new := jsonb_set(p_new, '{valid_from}', to_jsonb(p_now::timestamp), true);
    vf_txt := (p_now::timestamp)::text;
  END IF;

  -- Resolve/create history table
  SELECT to_regclass(v_schema || '.' || quote_ident(cand1)) INTO r;
  IF r IS NOT NULL THEN
    hist_tbl := cand1;
  ELSE
    SELECT to_regclass(v_schema || '.' || quote_ident(cand2)) INTO r;
    IF r IS NOT NULL THEN
      hist_tbl := cand2;
    ELSE
      EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL)', v_schema, cand2, v_schema, live_tbl);
      hist_tbl := cand2;
    END IF;
  END IF;

  -- Ensure version_no exists on history (defensive)
  PERFORM 1
  FROM information_schema.columns
  WHERE table_schema = v_schema AND table_name = hist_tbl AND column_name = 'version_no';
  IF NOT FOUND THEN
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN version_no integer NOT NULL DEFAULT 0', v_schema, hist_tbl);
  END IF;

  -- Build aligned, insertable column lists (intersection), excluding admin + generated + identity ALWAYS
  WITH hist_cols AS (
    SELECT column_name, ordinal_position
    FROM information_schema.columns
    WHERE table_schema = v_schema AND table_name = hist_tbl
      AND column_name NOT IN ('hist_id','live_id','archived_at','archived_by','version_no')
      AND COALESCE(is_generated, 'NEVER') = 'NEVER'
      AND NOT (is_identity = 'YES' AND identity_generation = 'ALWAYS')
  ),
  live_cols AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = v_schema AND table_name = live_tbl
      AND COALESCE(is_generated, 'NEVER') = 'NEVER'
      AND NOT (is_identity = 'YES' AND identity_generation = 'ALWAYS')
  ),
  inter AS (
    SELECT h.column_name
    FROM hist_cols h
    JOIN live_cols l USING (column_name)
    ORDER BY h.ordinal_position
  )
  SELECT string_agg(quote_ident(column_name), ', ') AS hist_cols,
         string_agg(quote_ident(column_name), ', ') AS live_cols
  INTO col_list_hist, col_list_live
  FROM inter;

  IF col_list_hist IS NULL OR col_list_live IS NULL THEN
    RAISE EXCEPTION 'No common insertable columns between %.% and %.%', v_schema, live_tbl, v_schema, hist_tbl;
  END IF;

  -- Compute next version number
  IF k2 IS NULL THEN
    EXECUTE format('SELECT COALESCE(MAX(version_no),0) FROM %I.%I WHERE %I=$1',
                   v_schema, hist_tbl, k1)
      INTO next_ver USING val1;
  ELSE
    EXECUTE format('SELECT COALESCE(MAX(version_no),0) FROM %I.%I WHERE %I=$1 AND %I=$2',
                   v_schema, hist_tbl, k1, k2)
      INTO next_ver USING val1, val2;
  END IF;
  next_ver := next_ver + 1;

  -- ================= FUTURE-DATED =================
  IF is_future THEN
    -- 1) Close current live slice to the start of the future slice (keep it current "now")
    IF k2 IS NULL THEN
      EXECUTE format(
        'UPDATE %I.%I
            SET valid_to = $2::timestamp
          WHERE %I=$1 AND is_current=true',
        v_schema, live_tbl, k1
      ) USING val1, (p_new->>'valid_from');
    ELSE
      EXECUTE format(
        'UPDATE %I.%I
            SET valid_to = $3::timestamp
          WHERE %I=$1 AND %I=$2 AND is_current=true',
        v_schema, live_tbl, k1, k2
      ) USING val1, val2, (p_new->>'valid_from');
    END IF;

    -- 2) Close any open-ended HISTORY slices for this key to the same boundary (fixes legacy leftovers)
    IF k2 IS NULL THEN
      EXECUTE format(
        'UPDATE %I.%I
            SET valid_to   = $2::timestamp,
                archived_at= COALESCE(archived_at, now()),
                archived_by= COALESCE(archived_by, current_user::text)
          WHERE %I=$1 AND valid_to IS NULL AND valid_from < $2::timestamp',
        v_schema, hist_tbl, k1
      ) USING val1, (p_new->>'valid_from');
    ELSE
      EXECUTE format(
        'UPDATE %I.%I
            SET valid_to   = $3::timestamp,
                archived_at= COALESCE(archived_at, now()),
                archived_by= COALESCE(archived_by, current_user::text)
          WHERE %I=$1 AND %I=$2 AND valid_to IS NULL AND valid_from < $3::timestamp',
        v_schema, hist_tbl, k1, k2
      ) USING val1, val2, (p_new->>'valid_from');
    END IF;

    -- 3) Assert (now safe: live and history are closed at the future boundary)
    PERFORM schema_poseidon_tst_16_adm._assert_no_overlap(
      live_tbl, k1, k2, val1, val2, vf_txt, vt_txt, NULL
    );

    -- 4) Write the future slice to HISTORY only
    EXECUTE format(
      'INSERT INTO %I.%I (%s)
       SELECT %s FROM (SELECT (jsonb_populate_record(NULL::%I.%I, $1)).*) r',
      v_schema, hist_tbl, col_list_hist, col_list_hist, v_schema, hist_tbl
    ) USING p_new;

    -- 5) Stamp version_no/archiver
    IF k2 IS NULL THEN
      EXECUTE format(
        'UPDATE %I.%I
            SET version_no=$1, archived_by=current_user::text
          WHERE %I=$2 AND valid_from=$3 AND valid_to IS NOT DISTINCT FROM $4',
        v_schema, hist_tbl, k1
      ) USING next_ver, val1, (p_new->>'valid_from')::timestamp, (p_new->>'valid_to')::timestamp;
    ELSE
      EXECUTE format(
        'UPDATE %I.%I
            SET version_no=$1, archived_by=current_user::text
          WHERE %I=$2 AND %I=$3 AND valid_from=$4 AND valid_to IS NOT DISTINCT FROM $5',
        v_schema, hist_tbl, k1, k2
      ) USING next_ver, val1, val2, (p_new->>'valid_from')::timestamp, (p_new->>'valid_to')::timestamp;
    END IF;

    RETURN true;  -- skip live insert
  END IF;

  -- ================= CURRENT-DATED =================
  -- 1) Archive prior current live row(s) to HISTORY
  IF k2 IS NULL THEN
    EXECUTE format($SQL$
      WITH old AS (
        SELECT * FROM %I.%I WHERE is_current=true AND %I=$1
      ),
      ins AS (
        INSERT INTO %I.%I (%s)
        SELECT %s FROM (
          SELECT (jsonb_populate_record(NULL::%I.%I, to_jsonb(o))).* FROM old o
        ) x
        RETURNING %I, %I, valid_from, valid_to
      )
      UPDATE %I.%I h
         SET live_id    = b.id,
             archived_at= now(),
             archived_by= current_user::text,
             version_no = $2,
             valid_to   = (jsonb_extract_path_text($3,'valid_from'))::timestamp
        FROM %I.%I b
        JOIN ins i
          ON i.%I = b.%I
         AND i.valid_from IS NOT DISTINCT FROM b.valid_from
         AND i.valid_to   IS NOT DISTINCT FROM b.valid_to
       WHERE h.%I=i.%I;
    $SQL$, v_schema, live_tbl, k1,
           v_schema, hist_tbl, col_list_hist, col_list_live, v_schema, hist_tbl,
           'hist_id', k1,
           v_schema, hist_tbl,
           v_schema, live_tbl, k1, k1,
           'hist_id','hist_id')
    USING val1, next_ver, p_new;

    -- 2) Close the live slice & unset current
    EXECUTE format(
      'UPDATE %I.%I
          SET is_current=false,
              valid_to   = (jsonb_extract_path_text($2,''valid_from''))::timestamp
        WHERE %I=$1 AND is_current=true',
      v_schema, live_tbl, k1
    ) USING val1, p_new;

  ELSE
    EXECUTE format($SQL$
      WITH old AS (
        SELECT * FROM %I.%I WHERE is_current=true AND %I=$1 AND %I=$2
      ),
      ins AS (
        INSERT INTO %I.%I (%s)
        SELECT %s FROM (
          SELECT (jsonb_populate_record(NULL::%I.%I, to_jsonb(o))).* FROM old o
        ) x
        RETURNING %I, %I, %I, valid_from, valid_to
      )
      UPDATE %I.%I h
         SET live_id    = b.id,
             archived_at= now(),
             archived_by= current_user::text,
             version_no = $3,
             valid_to   = (jsonb_extract_path_text($4,'valid_from'))::timestamp
        FROM %I.%I b
        JOIN ins i
          ON i.%I = b.%I
         AND i.%I = b.%I
         AND i.valid_from IS NOT DISTINCT FROM b.valid_from
         AND i.valid_to   IS NOT DISTINCT FROM b.valid_to
       WHERE h.%I=i.%I;
    $SQL$, v_schema, live_tbl, k1, k2,
           v_schema, hist_tbl, col_list_hist, col_list_live, v_schema, hist_tbl,
           'hist_id', k1, k2,
           v_schema, hist_tbl,
           v_schema, live_tbl, k1, k1, k2, k2,
           'hist_id','hist_id')
    USING val1, val2, next_ver, p_new;

    EXECUTE format(
      'UPDATE %I.%I
          SET is_current=false,
              valid_to   = (jsonb_extract_path_text($3,''valid_from''))::timestamp
        WHERE %I=$1 AND %I=$2 AND is_current=true',
      v_schema, live_tbl, k1, k2
    ) USING val1, val2, p_new;
  END IF;

  -- 3) Close any legacy open-ended HISTORY slice to NEW.valid_from (safety)
  IF k2 IS NULL THEN
    EXECUTE format(
      'UPDATE %I.%I
          SET valid_to   = $2::timestamp,
              archived_at= COALESCE(archived_at, now()),
              archived_by= COALESCE(archived_by, current_user::text)
        WHERE %I=$1 AND valid_to IS NULL AND valid_from < $2::timestamp',
      v_schema, hist_tbl, k1
    ) USING val1, (p_new->>'valid_from');
  ELSE
    EXECUTE format(
      'UPDATE %I.%I
          SET valid_to   = $3::timestamp,
              archived_at= COALESCE(archived_at, now()),
              archived_by= COALESCE(archived_by, current_user::text)
        WHERE %I=$1 AND %I=$2 AND valid_to IS NULL AND valid_from < $3::timestamp',
      v_schema, hist_tbl, k1, k2
    ) USING val1, val2, (p_new->>'valid_from');
  END IF;

  -- 4) Assert (now that live & history are closed at the boundary)
  PERFORM schema_poseidon_tst_16_adm._assert_no_overlap(
    live_tbl, k1, k2, val1, val2, vf_txt, vt_txt, NULL
  );

  RETURN false; -- proceed with live insert
END;
$$;

CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm.generic_versioning_to_history_fn()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  p_keys text[] := ARRAY[
    COALESCE(NULLIF(TG_ARGV[0],''), 'reference'),
    COALESCE(TG_ARGV[1], '')
  ];
  skip_live boolean;
BEGIN
  IF NEW.valid_from IS NULL THEN
    NEW.valid_from := now()::timestamp;
  END IF;

  skip_live := schema_poseidon_tst_16_adm._versioning_apply(
    TG_TABLE_SCHEMA, TG_TABLE_NAME, p_keys, to_jsonb(NEW), now()
  );

  IF skip_live THEN
    RETURN NULL;
  ELSE
    NEW.is_current := true;
    RETURN NEW;
  END IF;
END;
$$;

COMMIT;