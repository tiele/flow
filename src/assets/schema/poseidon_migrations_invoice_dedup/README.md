# Poseidon DB Migration Bundle (fixed 001/002 FK)

Date: 2025-08-31

Fixed ordering: 001 no longer references invoice_line; 002 creates invoice_line_version and adds the FK.

Apply in order:
001_output_membership.sql
002_invoice_line_versions.sql
003_execution_sig_and_clone.sql
004_usage_and_batch_history.sql
005_views_compat.sql
006_backfill_and_seed.sql
007_event_log.sql
008_event_hooks.sql
009_input_signature.sql
010_invoice_line_update_guard.sql
011_views_ops.sql
012_input_sig_refactor.sql
013_trace_no_lines.sql
014_trace_session_guard.sql
015_trace_auto_detect.sql
016_trace_auto_only.sql
