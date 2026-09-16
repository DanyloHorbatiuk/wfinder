-- v_active_courses: one row per currently active course version (SPEC §11).
-- skills comes from E-01 (course_skill); enrichment columns are added once
-- E-04 (course_enrichment) exists.
DROP VIEW IF EXISTS analytics.v_active_courses CASCADE;
CREATE VIEW analytics.v_active_courses AS
SELECT
    c.id,
    c.source,
    c.source_id,
    c.title,
    c.url,
    c.course_type,
    c.direction,
    c.format,
    c.level,
    c.price,
    c.date_start,
    c.date_end,
    c.status,
    c.country,
    c.city,
    c.languages,
    c.description,
    c.last_seen_at,
    c.active_from,
    COALESCE(
        (
            SELECT ARRAY_AGG(s.name ORDER BY s.name)
            FROM public.course_skill cs
            JOIN public.skill s ON s.id = cs.skill_id
            WHERE cs.course_id = c.id
        ),
        ARRAY[]::text[]
    ) AS skills
FROM public.courses c
WHERE c.active_to IS NULL;
