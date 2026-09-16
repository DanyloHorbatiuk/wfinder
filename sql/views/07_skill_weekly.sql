-- v_skill_weekly: one row per (week, skill) - SPEC §11 gives no precise
-- semantics beyond the column list, so this buckets by the week of
-- course.last_seen_at: "how many distinct postings were confirmed by a
-- snapshot to have this skill during that week."
DROP VIEW IF EXISTS analytics.v_skill_weekly CASCADE;
CREATE VIEW analytics.v_skill_weekly AS
SELECT
    date_trunc('week', c.last_seen_at)::date AS week,
    s.name AS skill,
    COUNT(DISTINCT (c.source, c.source_id)) AS postings_active
FROM public.skill s
JOIN public.course_skill cs ON cs.skill_id = s.id
JOIN public.courses c ON c.id = cs.course_id
WHERE c.last_seen_at IS NOT NULL
GROUP BY date_trunc('week', c.last_seen_at)::date, s.name;
