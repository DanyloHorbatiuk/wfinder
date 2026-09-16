-- v_skill_pairs: one row per skill pair co-occurring on a posting's latest
-- version (chronologically, regardless of active status), with >= 2 postings
-- (SPEC §11).
DROP VIEW IF EXISTS analytics.v_skill_pairs CASCADE;
CREATE VIEW analytics.v_skill_pairs AS
WITH last_versions AS (
    SELECT DISTINCT ON (c.source, c.source_id) c.id, c.source, c.source_id
    FROM public.courses c
    ORDER BY c.source, c.source_id, c.active_from DESC
),
last_version_skills AS (
    SELECT lv.source, lv.source_id, s.name AS skill
    FROM last_versions lv
    JOIN public.course_skill cs ON cs.course_id = lv.id
    JOIN public.skill s ON s.id = cs.skill_id
)
SELECT
    a.skill AS skill_a,
    b.skill AS skill_b,
    COUNT(DISTINCT (a.source, a.source_id)) AS postings
FROM last_version_skills a
JOIN last_version_skills b
    ON a.source = b.source AND a.source_id = b.source_id AND a.skill < b.skill
GROUP BY a.skill, b.skill
HAVING COUNT(DISTINCT (a.source, a.source_id)) >= 2;
