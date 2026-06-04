package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Client struct {
	client *goredis.Client
}

func New(addr string) *Client {
	return &Client{client: goredis.NewClient(&goredis.Options{
		Addr:         addr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		PoolSize:     16,
		MinIdleConns: 2,
	})}
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Client) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	score, err := c.client.ZScore(ctx, key, member).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return score, true, nil
}

func (c *Client) ZAdd(ctx context.Context, key string, score float64, member string) error {
	return c.client.ZAdd(ctx, key, goredis.Z{
		Score:  score,
		Member: member,
	}).Err()
}

func (c *Client) ZRevRank(ctx context.Context, key, member string) (int64, bool, error) {
	rank, err := c.client.ZRevRank(ctx, key, member).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return rank, true, nil
}
