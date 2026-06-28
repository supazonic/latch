package latch

import "context"

type Signal struct {
	Channel string
	Payload string
}

type Locker interface {
	Acquire(ctx context.Context, key int64) (bool, error)
	Release(ctx context.Context, key int64) error
}

type Signaler interface {
	Notify(ctx context.Context, channel, payload string) error
	Listen(ctx context.Context, channel string) (<-chan Signal, error)
}

type Coordinator interface {
	Locker
	Signaler
}
