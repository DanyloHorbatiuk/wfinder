-- Q3: Активні пропозиції в динаміці (лінія)
-- Dashboard: Ринок ІТ-стажувань. Filters: source, period.
SELECT day, source, active_end_of_day
FROM analytics.v_daily_activity
WHERE 1 = 1
  [[AND source = {{source}}]]
  [[AND day >= {{start_date}}]]
  [[AND day <= {{end_date}}]]
ORDER BY day
