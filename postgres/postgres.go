package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/supazonic/latch"
)

type Latch struct {
	db    *sql.DB
	mu    sync.Mutex
	conns map[int64]*sql.Conn // pinned connections holding advisory locks
}

/*
It's a compile-time interface check. It asserts that *Latch implements latch.Coordinator.
(*Latch)(nil) creates a nil pointer of type *Latch and assigns it to a blank identifier _ of type latch.Coordinator.
If *Latch is missing any method required by the interface, the code won't compile — you get an error pointing exactly to this line instead of somewhere at the call site where the type is actually used.
Without this line, the mismatch would only surface when someone tries to use a *Latch as a latch.Coordinator, which could be in a completely different file or even a different package.
*/
var _ latch.Coordinator = (*Latch)(nil)

func New(db *sql.DB) *Latch {
	return &Latch{
		db:    db,
		conns: make(map[int64]*sql.Conn),
	}
}

func (d *Latch) AcquireLock(ctx context.Context, key int64) (bool, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire connection: %w", err)
	}

	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
		conn.Close()
		return false, err
	}

	if acquired {
		d.mu.Lock()
		d.conns[key] = conn
		d.mu.Unlock()
	} else {
		conn.Close()
	}

	return acquired, nil
}

func (d *Latch) ReleaseLock(ctx context.Context, key int64) error {
	d.mu.Lock()
	conn, ok := d.conns[key]
	if ok {
		delete(d.conns, key)
	}
	d.mu.Unlock()

	if !ok {
		return fmt.Errorf("no lock held for key %d", key)
	}

	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", key)
	conn.Close()
	return err
}

// rawNotify sends a plain pg_notify with no round trip expectation.
func (d *Latch) rawNotify(ctx context.Context, event latch.Event, payload string) error {
	_, err := d.db.ExecContext(ctx, "SELECT pg_notify($1, $2)", event.String(), payload)
	return err
}

// Notify notifies event with payload and blocks until the pod that runs
// the matching Handler for event returns, then delivers that result back
// as the response. It relies on ctx for cancellation/timeout of the wait.
func (d *Latch) Notify(ctx context.Context, event latch.Event, payload string) (string, error) {
	id, err := newID()
	if err != nil {
		return "", fmt.Errorf("generate request id: %w", err)
	}
	replyEvent := latch.Event("latch_reply_" + id)

	sub, err := d.Subscribe(ctx, replyEvent)
	if err != nil {
		return "", fmt.Errorf("subscribe reply event: %w", err)
	}
	defer sub.Close()

	env := latch.Envelope{ID: id, ReplyEvent: replyEvent, Payload: payload}
	data, err := json.Marshal(env)
	if err != nil {
		return "", fmt.Errorf("marshal envelope: %w", err)
	}

	if err := d.rawNotify(ctx, event, string(data)); err != nil {
		return "", fmt.Errorf("notify: %w", err)
	}

	raw, err := sub.Wait(ctx)
	if err != nil {
		return "", fmt.Errorf("wait for response: %w", err)
	}

	var reply latch.Reply
	if err := json.Unmarshal([]byte(raw), &reply); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}
	if reply.Err != "" {
		return "", errors.New(reply.Err)
	}
	return reply.Payload, nil
}

// pumpInterval is how often a subscription forces a round trip on its
// pinned connection so lib/pq's read loop gets a chance to process any
// buffered NotificationResponse message and invoke the notification
// handler synchronously. lib/pq only surfaces notifications while it is
// actively reading from the wire for some other command, there is no
// background reader on a plain database/sql connection.
const pumpInterval = 200 * time.Millisecond

type subscription struct {
	conn     *sql.Conn
	notifyCh chan *pq.Notification
}

// Subscribe opens a single-event LISTEN on a dedicated database/sql
// connection and registers a lib/pq notification handler on the
// underlying driver connection. The LISTEN is confirmed by the server
// before Subscribe returns, so a Notify issued afterwards cannot be missed.
func (d *Latch) Subscribe(ctx context.Context, event latch.Event) (latch.Subscription, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}

	notifyCh := make(chan *pq.Notification, 1)
	if err := conn.Raw(func(c any) error {
		pq.SetNotificationHandler(c.(driver.Conn), func(n *pq.Notification) {
			notifyCh <- n
		})
		return nil
	}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("set notification handler: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "LISTEN "+pq.QuoteIdentifier(event.String())); err != nil {
		conn.Close()
		return nil, fmt.Errorf("listen %s: %w", event.String(), err)
	}

	return &subscription{conn: conn, notifyCh: notifyCh}, nil
}

func (s *subscription) Wait(ctx context.Context) (string, error) {
	ticker := time.NewTicker(pumpInterval)
	defer ticker.Stop()

	for {
		select {
		case n := <-s.notifyCh:
			return n.Extra, nil
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			// Forces a round trip so any pending NotificationResponse
			// message gets read and dispatched to notifyCh above.
			if _, err := s.conn.ExecContext(ctx, "SELECT 1"); err != nil {
				return "", fmt.Errorf("pump connection: %w", err)
			}
		}
	}
}

func (s *subscription) Close() error {
	return s.conn.Close()
}

// Listen subscribes to every channel in handlers on a single pinned
// connection and registers a lib/pq notification handler on the underlying
// driver connection. Incoming notifications are dispatched to the matching
// handler until ctx is cancelled. If the notification is a round-trip
// request (i.e. it carries a reply event), the handler's result is
// implicitly sent back on that event once the handler returns — handlers
// never do this themselves.
func (d *Latch) Listen(ctx context.Context, handlers map[latch.Event]latch.Handler) error {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}

	notifyCh := make(chan *pq.Notification, len(handlers))
	if err := conn.Raw(func(c any) error {
		pq.SetNotificationHandler(c.(driver.Conn), func(n *pq.Notification) {
			notifyCh <- n
		})
		return nil
	}); err != nil {
		conn.Close()
		return fmt.Errorf("set notification handler: %w", err)
	}

	for event := range handlers {
		if _, err := conn.ExecContext(ctx, "LISTEN "+pq.QuoteIdentifier(event.String())); err != nil {
			conn.Close()
			return fmt.Errorf("listen %s: %w", event.String(), err)
		}
	}

	go func() {
		defer conn.Close()
		ticker := time.NewTicker(pumpInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case n := <-notifyCh:
				h, ok := handlers[latch.Event(n.Channel)]
				if !ok {
					continue
				}
				go d.dispatch(ctx, h, n.Extra)
			case <-ticker.C:
				if _, err := conn.ExecContext(ctx, "SELECT 1"); err != nil {
					return
				}
			}
		}
	}()

	return nil
}

// dispatch runs h and, if the incoming notification was a round-trip
// request, implicitly notifies the reply event with the result.
func (d *Latch) dispatch(ctx context.Context, h latch.Handler, raw string) {
	var env latch.Envelope
	payload := raw
	isRoundTrip := false
	if err := json.Unmarshal([]byte(raw), &env); err == nil && env.ReplyEvent != "" {
		isRoundTrip = true
		payload = env.Payload
	}

	result, err := h(ctx, payload)
	if !isRoundTrip {
		return
	}

	reply := latch.Reply{ID: env.ID, Payload: result}
	if err != nil {
		reply.Err = err.Error()
	}

	data, err := json.Marshal(reply)
	if err != nil {
		return
	}
	d.rawNotify(ctx, env.ReplyEvent, string(data))
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
