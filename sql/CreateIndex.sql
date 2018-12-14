CREATE INDEX idx_compound4
ON comments (author, subreddit, link_id);

CREATE INDEX idx_compound5
ON comments (author, score);

CREATE INDEX idx_compound6
ON comments (subreddit, score);

CREATE INDEX idx_compound7
ON comments (author, link_id);

CREATE INDEX idx_compound8
ON comments (author, subreddit);

