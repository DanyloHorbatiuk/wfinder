-- Q10: Отримано записів за знімками по джерелах (лінія)
-- Dashboard: Здоров'я пайплайна. Filter: source.
SELECT snapshot_at, source, received
FROM analytics.v_pipeline_health
WHERE 1 = 1
  [[AND source = {{source}}]]
ORDER BY snapshot_at
