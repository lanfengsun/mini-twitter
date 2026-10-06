-- Identical schema on every shard. Which shard a row lives on is decided by the
-- application (internal/shard): users by hash(username); posts, likes and replies
-- by hash(post_id); following by hash(follower_id); followers by hash(followee_id).

CREATE TABLE users (
    id            BIGINT PRIMARY KEY,
    username      TEXT        NOT NULL UNIQUE,
    password_hash TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE posts (
    id          BIGINT PRIMARY KEY,          -- time-sortable id (internal/idgen)
    author_id   BIGINT      NOT NULL,
    author_name TEXT        NOT NULL,        -- denormalised: feeds never join users
    body        TEXT        NOT NULL,
    like_count  INT         NOT NULL DEFAULT 0,   -- flushed asynchronously from Redis
    reply_count INT         NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX posts_author_idx ON posts (author_id, id DESC);

-- "who do I follow": sharded by follower_id
CREATE TABLE following (
    follower_id BIGINT NOT NULL,
    followee_id BIGINT NOT NULL,
    PRIMARY KEY (follower_id, followee_id)
);

-- "who follows me" (fan-out targets): the same edge, sharded by followee_id
CREATE TABLE followers (
    followee_id BIGINT NOT NULL,
    follower_id BIGINT NOT NULL,
    PRIMARY KEY (followee_id, follower_id)
);

CREATE TABLE likes (
    post_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    PRIMARY KEY (post_id, user_id)
);

CREATE TABLE replies (
    id          BIGINT PRIMARY KEY,
    post_id     BIGINT      NOT NULL,
    author_id   BIGINT      NOT NULL,
    author_name TEXT        NOT NULL,
    body        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX replies_post_idx ON replies (post_id, id DESC);
