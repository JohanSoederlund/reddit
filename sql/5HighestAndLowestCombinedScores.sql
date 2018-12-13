SELECT 
    author, total_score
FROM
    (SELECT 
        author, SUM(score) AS total_score
    FROM
        comments
    GROUP BY author) s
ORDER BY total_score DESC
LIMIT 1;

SELECT 
    author, total_score
    FROM
    (SELECT 
        author, SUM(score) AS total_score
    FROM
        comments
    GROUP BY author) s
ORDER BY total_score ASC
LIMIT 1;