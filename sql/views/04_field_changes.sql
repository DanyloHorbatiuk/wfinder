-- v_field_changes: one row per changed tracked field between adjacent versions
-- of a posting, where the previous version was closed with close_reason='changed'
-- (SPEC §11). Tracked fields match core/hashing.py's TRACKED_FIELDS.
DROP VIEW IF EXISTS analytics.v_field_changes CASCADE;
CREATE VIEW analytics.v_field_changes AS
WITH ordered AS (
    SELECT
        c.source,
        c.source_id,
        c.active_from,
        c.title, c.url, c.course_type, c.direction, c.format, c.level, c.price,
        c.date_start, c.date_end, c.status, c.country, c.city, c.languages, c.description,
        LAG(c.title) OVER w AS prev_title,
        LAG(c.url) OVER w AS prev_url,
        LAG(c.course_type) OVER w AS prev_course_type,
        LAG(c.direction) OVER w AS prev_direction,
        LAG(c.format) OVER w AS prev_format,
        LAG(c.level) OVER w AS prev_level,
        LAG(c.price) OVER w AS prev_price,
        LAG(c.date_start) OVER w AS prev_date_start,
        LAG(c.date_end) OVER w AS prev_date_end,
        LAG(c.status) OVER w AS prev_status,
        LAG(c.country) OVER w AS prev_country,
        LAG(c.city) OVER w AS prev_city,
        LAG(c.languages) OVER w AS prev_languages,
        LAG(c.description) OVER w AS prev_description,
        LAG(c.close_reason) OVER w AS prev_close_reason
    FROM public.courses c
    WINDOW w AS (PARTITION BY c.source, c.source_id ORDER BY c.active_from)
)
SELECT
    o.source,
    o.source_id,
    o.active_from AS changed_at,
    v.field_name
FROM ordered o
CROSS JOIN LATERAL (VALUES
    ('title', o.title IS DISTINCT FROM o.prev_title),
    ('url', o.url IS DISTINCT FROM o.prev_url),
    ('course_type', o.course_type IS DISTINCT FROM o.prev_course_type),
    ('direction', o.direction IS DISTINCT FROM o.prev_direction),
    ('format', o.format IS DISTINCT FROM o.prev_format),
    ('level', o.level IS DISTINCT FROM o.prev_level),
    ('price', o.price IS DISTINCT FROM o.prev_price),
    ('date_start', o.date_start IS DISTINCT FROM o.prev_date_start),
    ('date_end', o.date_end IS DISTINCT FROM o.prev_date_end),
    ('status', o.status IS DISTINCT FROM o.prev_status),
    ('country', o.country IS DISTINCT FROM o.prev_country),
    ('city', o.city IS DISTINCT FROM o.prev_city),
    ('languages', o.languages IS DISTINCT FROM o.prev_languages),
    ('description', o.description IS DISTINCT FROM o.prev_description)
) AS v(field_name, is_changed)
WHERE o.prev_close_reason = 'changed'
  AND v.is_changed;
