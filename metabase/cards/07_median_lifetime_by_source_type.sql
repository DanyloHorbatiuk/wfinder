-- Q7: Медіанна тривалість життя за джерелом і типом (таблиця / стовпчики)
-- Dashboard: Ринок ІТ-стажувань.
-- v_posting_episodes has no course_type, so join back to each posting's
-- chronologically last version for it. Median restricted to closed,
-- non-left-censored episodes per SPEC §11.
WITH last_versions AS (
    SELECT DISTINCT ON (source, source_id) source, source_id, course_type
    FROM public.courses
    ORDER BY source, source_id, active_from DESC
)
SELECT
    e.source,
    lv.course_type,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY e.lifetime_days) AS median_lifetime_days,
    count(*) AS episodes
FROM analytics.v_posting_episodes e
JOIN last_versions lv ON lv.source = e.source AND lv.source_id = e.source_id
WHERE e.removed_at IS NOT NULL AND NOT e.left_censored
GROUP BY e.source, lv.course_type
ORDER BY e.source, lv.course_type
