package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	pgxstdlib "github.com/jackc/pgx/v5/stdlib"
	"github.com/supazonic/latch"
)

type Latch struct {
	db    *sql.DB
	mu    sync.Mutex
	conns map[int64]*sql.Conn // pinned connections holding advisory locks
}

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

func (d *Latch) Notify(ctx context.Context, channel, payload string) error {
	_, err := d.db.ExecContext(ctx, "SELECT pg_notify($1, $2)", channel, payload)
	return err
}

// Listen subscribes to every channel in handlers on a single pinned connection.
// Incoming notifications are dispatched to the matching handler until ctx is cancelled.
func (d *Latch) Listen(ctx context.Context, handlers map[string]latch.Handler) error {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}

	for channel := range handlers {
		if _, err := conn.ExecContext(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
			conn.Close()
			return fmt.Errorf("listen %s: %w", channel, err)
		}
	}

	go func() {
		defer conn.Close()
		conn.Raw(func(c any) error {
			pgxConn := c.(*pgxstdlib.Conn).Conn()
			for {
				n, err := pgxConn.WaitForNotification(ctx)
				if err != nil {
					return nil
				}
				if h, ok := handlers[n.Channel]; ok {
					h(ctx, n.Payload)
				}
			}
		})
	}()

	return nil
}
