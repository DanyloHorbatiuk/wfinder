-- Q1: Активні пропозиції зараз (число)
-- Dashboard: Ринок ІТ-стажувань. Filter: source (optional).
SELECT count(*) AS active_postings
FROM analytics.v_active_courses
WHERE 1 = 1
  [[AND source = {{source}}]]
