SELECT 
    author, subreddit
FROM
    comments c1
WHERE
    NOT EXISTS( SELECT 
            *
        FROM
            comments
        WHERE
            subreddit <> c1.subreddit
                AND author = c1.author)
                ORDER BY author;
