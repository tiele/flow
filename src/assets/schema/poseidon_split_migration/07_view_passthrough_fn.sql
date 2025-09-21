-- 07) Updatable view passthrough trigger
-- Schema: schema_poseidon_tst_16_adm | Generated: 2025-08-31T16:10:29.827014
BEGIN;
CREATE OR REPLACE FUNCTION schema_poseidon_tst_16_adm._view_passthrough_trg()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE live_tbl text := TG_ARGV[0];
BEGIN
  IF TG_OP='INSERT' THEN
    EXECUTE format('INSERT INTO %s SELECT ($1).*', live_tbl) USING NEW;
    RETURN NEW;
  ELSIF TG_OP='UPDATE' THEN
    EXECUTE format('UPDATE %s SET %s WHERE id=$2',
                   live_tbl,
                   (SELECT string_agg(format('%1$s = ($1).%1$s', quote_ident(attname)), ', ')
                    FROM pg_attribute
                    WHERE attrelid = live_tbl::regclass
                      AND attnum > 0 AND NOT attisdropped
                      AND attname <> 'id'))
      USING NEW, OLD.id;
    RETURN NEW;
  ELSIF TG_OP='DELETE' THEN
    EXECUTE format('DELETE FROM %s WHERE id=$1', live_tbl) USING OLD.id;
    RETURN OLD;
  END IF;
  RETURN NULL;
END;
$$;
COMMIT;
