-- Q2: Нові пропозиції за днями (стовпчики за джерелами)
-- Dashboard: Ринок ІТ-стажувань. Filters: source, period.
SELECT day, source, new
FROM analytics.v_daily_activity
WHERE 1 = 1
  [[AND source = {{source}}]]
  [[AND day >= {{start_date}}]]
  [[AND day <= {{end_date}}]]
ORDER BY day
