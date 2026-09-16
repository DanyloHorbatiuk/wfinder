-- v_pipeline_health: one row per processed snapshot, joining load_stats with the
-- owning file_record's status/error_message (SPEC §11, E-08).
DROP VIEW IF EXISTS analytics.v_pipeline_health CASCADE;
CREATE VIEW analytics.v_pipeline_health AS
SELECT
    ls.id,
    ls.file_record_id,
    ls.source,
    ls.snapshot_at,
    ls.run_id,
    ls.received,
    ls.rejected,
    ls.duplicates,
    ls.inserted,
    ls.changed,
    ls.unchanged,
    ls.closed,
    ls.closures_blocked,
    ls.block_reason,
    ls.parser_version,
    ls.duration_ms,
    ls.created_at,
    fr.status AS file_status,
    fr.error_message
FROM public.load_stats ls
JOIN public.file_record fr ON fr.id = ls.file_record_id;
