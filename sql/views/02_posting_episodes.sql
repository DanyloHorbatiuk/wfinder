-- v_posting_episodes: one row per episode - a continuous run of versions for a
-- (source, source_id) from its first version to a close_reason='removed'
-- closure (SPEC §4.1, §11). A new episode starts if the posting reappears.
DROP VIEW IF EXISTS analytics.v_posting_episodes CASCADE;
CREATE VIEW analytics.v_posting_episodes AS
WITH episode_marks AS (
    SELECT
        c.source,
        c.source_id,
        c.title,
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
episodes AS (
    SELECT
        source,
        source_id,
        episode_num,
        MIN(active_from) AS first_seen,
        MAX(active_to) FILTER (WHERE close_reason = 'removed') AS removed_at,
        BOOL_OR(active_to IS NULL) AS is_active,
        COUNT(*) AS versions_count,
        (ARRAY_AGG(title ORDER BY active_from DESC))[1] AS title
    FROM numbered
    GROUP BY source, source_id, episode_num
),
source_bounds AS (
    SELECT source, MIN(snapshot_at) AS first_snapshot_at, MAX(snapshot_at) AS last_snapshot_at
    FROM public.load_stats
    GROUP BY source
)
SELECT
    e.source,
    e.source_id,
    e.title,
    e.first_seen,
    e.removed_at,
    e.is_active,
    ROUND(
        EXTRACT(EPOCH FROM (COALESCE(e.removed_at, sb.last_snapshot_at) - e.first_seen)) / 86400.0,
        1
    ) AS lifetime_days,
    e.versions_count,
    (e.first_seen = sb.first_snapshot_at) AS left_censored
FROM episodes e
LEFT JOIN source_bounds sb ON sb.source = e.source;
