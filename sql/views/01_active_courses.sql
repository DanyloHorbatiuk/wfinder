-- v_active_courses: one row per currently active course version (SPEC §11).
-- Skills/enrichment columns are added here once E-01/E-04 exist.
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
    c.active_from
FROM public.courses c
WHERE c.active_to IS NULL;
