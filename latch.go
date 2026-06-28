package latch

import "context"

type Signal struct {
	Channel string
	Payload string
}

type Locker interface {
	AcquireLock(ctx context.Context, key int64) (bool, error)
	ReleaseLock(ctx context.Context, key int64) error
}

// Handler is called when a notification arrives on the subscribed channel.
type Handler func(ctx context.Context, payload string)

type Signaler interface {
	Notify(ctx context.Context, channel, payload string) error
	Listen(ctx context.Context, handlers map[string]Handler) error
}

type Coordinator interface {
	Locker
	Signaler
}
