-- Q8: Розподіл тривалості життя, кошики по 7 днів (гістограма)
-- Dashboard: Ринок ІТ-стажувань. Filter: source.
SELECT
    floor(lifetime_days / 7) * 7 AS bucket_start_days,
    count(*) AS episodes
FROM analytics.v_posting_episodes
WHERE removed_at IS NOT NULL AND NOT left_censored
  [[AND source = {{source}}]]
GROUP BY floor(lifetime_days / 7)
ORDER BY bucket_start_days
