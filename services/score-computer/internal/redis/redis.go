// Package redis implements redis behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package redis

import (
	"context"
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
