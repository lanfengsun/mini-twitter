// Package events defines the messages that travel through the Redis Stream.
package events

// Event types (the "t" field of a stream entry).
const (
	TypePost   = "post"
	TypeFollow = "follow"
	TypeLike   = "like"
	TypeReply  = "reply"
)

type Post struct {
	ID         int64  `json:"id"`
	AuthorID   int64  `json:"author_id"`
	AuthorName string `json:"author_name"`
	Body       string `json:"body"`
}

type Follow struct {
	FollowerID int64 `json:"follower_id"`
	FolloweeID int64 `json:"followee_id"`
}

type Like struct {
	PostID int64 `json:"post_id"`
	UserID int64 `json:"user_id"`
}

type Reply struct {
	ID         int64  `json:"id"`
	PostID     int64  `json:"post_id"`
	AuthorID   int64  `json:"author_id"`
	AuthorName string `json:"author_name"`
	Body       string `json:"body"`
}
