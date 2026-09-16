-- Q9: Поля, що змінюються найчастіше (стовпчики)
-- Dashboard: Ринок ІТ-стажувань. Filters: source, period.
SELECT field_name, count(*) AS changes
FROM analytics.v_field_changes
WHERE 1 = 1
  [[AND source = {{source}}]]
  [[AND changed_at >= {{start_date}}]]
  [[AND changed_at <= {{end_date}}]]
GROUP BY field_name
ORDER BY changes DESC
