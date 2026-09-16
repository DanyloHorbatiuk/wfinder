-- Q5: Тренд топ-5 технологій за тижнями (лінія)
-- Dashboard: Ринок ІТ-стажувань. Filter: period.
WITH top5 AS (
    SELECT skill FROM analytics.v_skill_counts ORDER BY active_postings DESC LIMIT 5
)
SELECT w.week, w.skill, w.postings_active
FROM analytics.v_skill_weekly w
JOIN top5 t ON t.skill = w.skill
WHERE 1 = 1
  [[AND w.week >= {{start_date}}]]
  [[AND w.week <= {{end_date}}]]
ORDER BY w.week
