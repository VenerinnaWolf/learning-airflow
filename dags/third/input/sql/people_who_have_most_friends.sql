--	+----------------+---------+
--	| Column Name    | Type    |
--	+----------------+---------+
--	| requester_id   | int     |
--	| accepter_id    | int     |
--	| accept_date    | date    |
--	+----------------+---------+
--	(requester_id, accepter_id) is the primary key (combination of columns with unique values) for this table.
--	This table contains the ID of the user who sent the request, the ID of the user who received the request, and the date when the request was accepted.
--	 
--  Write a solution to find the people who have the most friends and the most friends number.

-- --------
-- Solution:

-- WITH requesters AS (
--     SELECT 
--         requester_id id, 
--         COUNT(requester_id) requester_num 
--     FROM RequestAccepted
--     GROUP BY requester_id
-- ),
-- accepters AS (
--     SELECT 
--         accepter_id id, 
--         COUNT(accepter_id) accepter_num 
--     FROM RequestAccepted
--     GROUP BY accepter_id
-- )
-- SELECT 
--     id, 
--     (COALESCE(requester_num, 0) + COALESCE(accepter_num, 0)) num
-- FROM requesters r FULL OUTER JOIN accepters a USING (id)
-- ORDER BY num DESC NULLS LAST
-- LIMIT 1

WITH all_ids AS (
	SELECT requester_id AS id 
	FROM request_accepted ra
	UNION ALL
	SELECT accepter_id AS id
	FROM request_accepted ra
)
SELECT id, COUNT(id) num
FROM all_ids
GROUP BY id
ORDER BY num DESC
--LIMIT 1