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
// Its return value is the response for the round trip: once Handler
// returns, the package implicitly sends the result back to whichever pod
// is blocked in Notify for that request. Handler authors never send the
// response themselves.
type Handler func(ctx context.Context, payload string) (string, error)

type Event string

func (e Event) String() string {
	return string(e)
}

// Envelope wraps a request payload with the event the executing pod
// should reply on, so a round trip can be correlated with its response.
type Envelope struct {
	ID         string `json:"id"`
	ReplyEvent Event  `json:"reply_event"`
	Payload    string `json:"payload"`
}

// Reply carries the result of a Handler call back to the pod blocked in
// Notify. Sending it is an implementation detail of the package; it is
// never constructed by Handler authors.
type Reply struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
	Err     string `json:"err,omitempty"`
}

// Subscription represents a single-event subscription created by
// Signaler.Subscribe. Wait blocks until a notification arrives for the
// subscribed event or ctx is done. Subscribe guarantees the subscription
// is listening before it returns, so a Notify issued afterwards cannot be
// missed.
type Subscription interface {
	Wait(ctx context.Context) (string, error)
	Close() error
}

// Signaler notifies events and blocks for a round trip: Notify does not
// return until the pod that runs the matching Handler for event returns,
// at which point its result is delivered back as the response.
type Signaler interface {
	Notify(ctx context.Context, event Event, payload string) (string, error)
	Subscribe(ctx context.Context, event Event) (Subscription, error)
	Listen(ctx context.Context, handlers map[Event]Handler) error
}

type Coordinator interface {
	Locker
	Signaler
}
