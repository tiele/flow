-- 06) Promotion-on-read function (two-step archive)
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:10:29.827014
BEGIN;
CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm._promote_overdue_generic(
  p_tbl text,
  p_key1 text,
  p_key2 text DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  live_tbl text := format('schema_poseidon_tst_16_adm.%s_live', p_tbl);
  hist_tbl text := format('schema_poseidon_tst_16_adm.%s_history', p_tbl);
  promoted_cnt int := 0;
BEGIN
  IF p_key2 IS NULL THEN
    EXECUTE format($SQL$
      WITH expired AS (
        SELECT l.* FROM %s l
        WHERE l.is_current=true AND l.valid_to IS NOT NULL AND l.valid_to <= now()
      ),
      arch_ins AS (
        INSERT INTO %s SELECT e.* FROM expired e
        RETURNING hist_id, %I, valid_from, valid_to
      ),
      arch_upd AS (
        UPDATE %s h
           SET live_id=b.id,
               archived_at=now(),
               archived_by=current_user::text,
               version_no=COALESCE((SELECT MAX(h2.version_no)+1 FROM %s h2 WHERE h2.%I=b.%I),1)
          FROM %s b
          JOIN arch_ins i ON i.%I=b.%I AND i.valid_from IS NOT DISTINCT FROM b.valid_from AND i.valid_to IS NOT DISTINCT FROM b.valid_to
         WHERE h.hist_id=i.hist_id
         RETURNING h.%I, h.valid_to
      ),
      candidates AS (
        SELECT h.* FROM %s h
        JOIN arch_upd a ON a.%I=h.%I
        WHERE h.valid_from >= a.valid_to AND h.valid_from <= now() AND h.valid_to IS NULL
        ORDER BY h.%I, h.valid_from ASC
      ),
      chosen AS (
        SELECT DISTINCT ON (%I) * FROM candidates ORDER BY %I, valid_from ASC
      ),
      remove_live AS (
        DELETE FROM %s l WHERE l.is_current=true AND l.valid_to IS NOT NULL AND l.valid_to <= now()
      ),
      promote AS (
        INSERT INTO %s SELECT (c).* FROM chosen c
        ON CONFLICT (%I) DO NOTHING
        RETURNING %I
      )
      SELECT count(*) FROM promote
    $SQL$, live_tbl, hist_tbl, p_key1, hist_tbl, hist_tbl, p_key1, p_key1, live_tbl, p_key1, p_key1, p_key1, hist_tbl, p_key1, p_key1, p_key1, p_key1, live_tbl, live_tbl, p_key1, p_key1)
    INTO promoted_cnt;
    RETURN COALESCE(promoted_cnt,0);
  END IF;

  EXECUTE format($SQL$
    WITH expired AS (
      SELECT l.* FROM %s l
      WHERE l.is_current=true AND l.valid_to IS NOT NULL AND l.valid_to <= now()
    ),
    arch_ins AS (
      INSERT INTO %s SELECT e.* FROM expired e
      RETURNING hist_id, %I, %I, valid_from, valid_to
    ),
    arch_upd AS (
      UPDATE %s h
         SET live_id=b.id,
             archived_at=now(),
             archived_by=current_user::text,
             version_no=COALESCE((SELECT MAX(h2.version_no)+1 FROM %s h2 WHERE h2.%I=b.%I AND h2.%I=b.%I),1)
        FROM %s b
        JOIN arch_ins i ON i.%I=b.%I AND i.%I=b.%I AND i.valid_from IS NOT DISTINCT FROM b.valid_from AND i.valid_to IS NOT DISTINCT FROM b.valid_to
       WHERE h.hist_id=i.hist_id
       RETURNING h.%I, h.%I, h.valid_to
    ),
    candidates AS (
      SELECT h.* FROM %s h
      JOIN arch_upd a ON a.%I=h.%I AND a.%I=h.%I
      WHERE h.valid_from >= a.valid_to AND h.valid_from <= now() AND h.valid_to IS NULL
      ORDER BY h.%I, h.%I, h.valid_from ASC
    ),
    chosen AS (
      SELECT DISTINCT ON (%I,%I) * FROM candidates ORDER BY %I, %I, valid_from ASC
    ),
    remove_live AS (
      DELETE FROM %s l WHERE l.is_current=true AND l.valid_to IS NOT NULL AND l.valid_to <= now()
    ),
    promote AS (
      INSERT INTO %s SELECT (c).* FROM chosen c
      ON CONFLICT (%I,%I) DO NOTHING
      RETURNING %I,%I
    )
    SELECT count(*) FROM promote
  $SQL$, live_tbl, hist_tbl, p_key1, p_key2, hist_tbl, hist_tbl, p_key1, p_key1, p_key2, p_key2, live_tbl, p_key1, p_key1, p_key2, p_key2, p_key1, p_key2, p_key1, p_key2, hist_tbl, p_key1, p_key1, p_key2, p_key2, p_key1, p_key2, p_key1, p_key2, live_tbl, live_tbl, live_tbl, p_key1, p_key2, p_key1, p_key2)
  INTO promoted_cnt;

  RETURN COALESCE(promoted_cnt,0);
END;
$$;
COMMIT;
