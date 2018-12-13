SELECT 
    c1.author, c1.subreddit
FROM
    comments c1
        INNER JOIN
    (SELECT 
        author, subreddit
    FROM
        comments
    WHERE
        link_id = 't3_5yba3') AS u1
WHERE
    c1.author = u1.author
        AND u1.subreddit <> c1.subreddit
ORDER BY c1.author