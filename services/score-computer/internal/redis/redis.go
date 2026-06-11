// Package redis implements redis behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Client groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Client struct {
	client *goredis.Client
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Client) Close() error {
	return c.client.Close()
}

// Ping applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// ZScore applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// ZAdd applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Client) ZAdd(ctx context.Context, key string, score float64, member string) error {
	return c.client.ZAdd(ctx, key, goredis.Z{
		Score:  score,
		Member: member,
	}).Err()
}

// ZRevRank applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
