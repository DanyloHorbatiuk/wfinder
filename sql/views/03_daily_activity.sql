-- v_daily_activity: one row per (day, source) between that source's first and
-- last snapshot (SPEC §11). new/removed count episode starts/ends that day;
-- changed counts versions closed with close_reason='changed'; active_end_of_day
-- counts postings active at the end of that day.
DROP VIEW IF EXISTS analytics.v_daily_activity CASCADE;
CREATE VIEW analytics.v_daily_activity AS
WITH source_bounds AS (
    SELECT source, MIN(snapshot_at)::date AS first_day, MAX(snapshot_at)::date AS last_day
    FROM public.load_stats
    GROUP BY source
),
days AS (
    SELECT sb.source, gs::date AS day
    FROM source_bounds sb
    CROSS JOIN LATERAL generate_series(sb.first_day, sb.last_day, interval '1 day') AS gs
),
episode_marks AS (
    SELECT
        c.source,
        c.source_id,
        c.active_from,
        c.active_to,
        c.close_reason,
        LAG(c.close_reason) OVER (PARTITION BY c.source, c.source_id ORDER BY c.active_from) AS prev_close_reason
    FROM public.courses c
),
numbered AS (
    SELECT
        *,
        SUM(CASE WHEN prev_close_reason = 'removed' THEN 1 ELSE 0 END)
            OVER (PARTITION BY source, source_id ORDER BY active_from) AS episode_num
    FROM episode_marks
),
episode_bounds AS (
    SELECT
        source,
        source_id,
        episode_num,
        MIN(active_from) AS episode_start,
        MAX(active_to) FILTER (WHERE close_reason = 'removed') AS episode_end
    FROM numbered
    GROUP BY source, source_id, episode_num
),
new_counts AS (
    SELECT source, episode_start::date AS day, COUNT(*) AS new_count
    FROM episode_bounds
    GROUP BY source, episode_start::date
),
removed_counts AS (
    SELECT source, episode_end::date AS day, COUNT(*) AS removed_count
    FROM episode_bounds
    WHERE episode_end IS NOT NULL
    GROUP BY source, episode_end::date
),
changed_counts AS (
    SELECT source, active_to::date AS day, COUNT(*) AS changed_count
    FROM public.courses
    WHERE close_reason = 'changed'
    GROUP BY source, active_to::date
),
active_counts AS (
    SELECT d.source, d.day, COUNT(c.id) AS active_count
    FROM days d
    LEFT JOIN public.courses c
        ON c.source = d.source
       AND c.active_from::date <= d.day
       AND (c.active_to IS NULL OR c.active_to::date > d.day)
    GROUP BY d.source, d.day
)
SELECT
    d.day,
    d.source,
    COALESCE(nc.new_count, 0) AS new,
    COALESCE(cc.changed_count, 0) AS changed,
    COALESCE(rc.removed_count, 0) AS removed,
    COALESCE(ac.active_count, 0) AS active_end_of_day
FROM days d
LEFT JOIN new_counts nc ON nc.source = d.source AND nc.day = d.day
LEFT JOIN changed_counts cc ON cc.source = d.source AND cc.day = d.day
LEFT JOIN removed_counts rc ON rc.source = d.source AND rc.day = d.day
LEFT JOIN active_counts ac ON ac.source = d.source AND ac.day = d.day
ORDER BY d.source, d.day;
