SELECT DISTINCT
    c1.author
FROM
    comments c1
		INNER JOIN
    (SELECT 
        author, link_id
    FROM
        comments
    WHERE
        author = 'johan') AS u1
WHERE
    c1.link_id = u1.link_id AND
	c1.author <> u1.author
ORDER BY c1.author