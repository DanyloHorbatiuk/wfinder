-- Q6: Технології, що зустрічаються разом (таблиця)
-- Dashboard: Ринок ІТ-стажувань.
SELECT skill_a, skill_b, postings
FROM analytics.v_skill_pairs
ORDER BY postings DESC
LIMIT 50
