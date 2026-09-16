-- Q12: Частка відхилених записів, після E-05 (лінія)
-- Dashboard: Здоров'я пайплайна. Filter: source.
SELECT
    snapshot_at,
    source,
    CASE WHEN received > 0 THEN round(100.0 * rejected / received, 1) ELSE 0 END AS rejected_share_pct
FROM analytics.v_pipeline_health
WHERE 1 = 1
  [[AND source = {{source}}]]
ORDER BY snapshot_at
