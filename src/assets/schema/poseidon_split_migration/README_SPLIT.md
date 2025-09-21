# Poseidon Migration — Split into Small Steps

**Schema:** `schema_poseidon_tst_16_adm`  
**Apply in this exact order:**

1. `00_reconcile_uniques.sql` – normalize UNIQUE(reference) on base tables (no-skip).
2. `01_create_history_tables.sql` – create `*_history` (+ columns, range, indexes).
3. `02_archive_existing_to_history.sql` – move non-current / end-dated rows to history (two-step) and delete from base; backfill `version_no` from legacy `-V####`.
4. `03_add_live_constraints_and_ranges.sql` – partial unique on current rows, generated `valid_range`, and window checks.
5. `04_overlap_guard_fn.sql` – `_assert_no_overlap(...)` function.
6. `05_versioning_trigger_fn.sql` – `generic_versioning_to_history_fn(...)` and attach BEFORE INSERT triggers on **base** tables.
7. `06_promote_on_read_fn.sql` – `_promote_overdue_generic(...)` for lazy promotion.
8. `07_view_passthrough_fn.sql` – `_view_passthrough_trg()` for updatable views.
9. `08_rename_to_live_and_create_views.sql` – rename base → `*_live`, reattach versioning triggers to `*_live`, create views with original names, and add INSTEAD OF triggers.

## Notes
- These scripts are **idempotent-ish** and defensive, but keep the order to avoid dependency issues.
- No references to non-existent `creation_user` / `update_user`; we use `current_user` for `archived_by`.
- Two-step insert+update is used everywhere we move rows across tables to avoid column-order/type issues.
- After step 9, your app keeps using the original table names (now views) with lazy promotion on read.

If you want a single “driver” file that `\i` includes each step and stops on first error, say the word and I’ll add it.
