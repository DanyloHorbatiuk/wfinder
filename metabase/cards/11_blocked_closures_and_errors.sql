-- Q11: Заблоковані закриття й помилки файлів (таблиця)
-- Dashboard: Здоров'я пайплайна.
SELECT snapshot_at, source, file_status, error_message, closures_blocked, block_reason
FROM analytics.v_pipeline_health
WHERE closures_blocked = true OR file_status = 'error'
ORDER BY snapshot_at DESC
