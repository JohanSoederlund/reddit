SELECT 
	max_score,
    subreddit
FROM
    (SELECT 
        subreddit, MAX(score) AS max_score
    FROM
        comments
    GROUP BY subreddit) s
ORDER BY max_score DESC
LIMIT 1;

SELECT 
    subreddit, min_score
FROM
    (SELECT 
        subreddit, MIN(score) AS min_score
    FROM
        comments
    GROUP BY subreddit) s
ORDER BY min_score ASC
LIMIT 1;

