-- v_skill_counts: one row per skill (SPEC §11).
DROP VIEW IF EXISTS analytics.v_skill_counts CASCADE;
CREATE VIEW analytics.v_skill_counts AS
SELECT
    s.name AS skill,
    s.category,
    COUNT(*) FILTER (WHERE c.active_to IS NULL) AS active_postings,
    COUNT(DISTINCT (c.source, c.source_id)) AS all_time_postings
FROM public.skill s
JOIN public.course_skill cs ON cs.skill_id = s.id
JOIN public.courses c ON c.id = cs.course_id
GROUP BY s.name, s.category;
