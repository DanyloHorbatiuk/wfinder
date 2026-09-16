-- Q4: Топ-15 технологій серед активних (горизонтальні стовпчики)
-- Dashboard: Ринок ІТ-стажувань. No source filter: v_skill_counts is not
-- source-scoped (a skill can come from postings across sources).
SELECT skill, category, active_postings
FROM analytics.v_skill_counts
ORDER BY active_postings DESC
LIMIT 15
