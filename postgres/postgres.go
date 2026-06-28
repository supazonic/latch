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

// DB wraps *sql.DB and implements latch.Coordinator.
// The underlying *sql.DB must use the pgx driver (stdlib.OpenDB or sql.Open("pgx", ...)).
type DB struct {
	db    *sql.DB
	mu    sync.Mutex
	conns map[int64]*sql.Conn // pinned connections holding advisory locks
}

var _ latch.Coordinator = (*DB)(nil)

func New(db *sql.DB) *DB {
	return &DB{
		db:    db,
		conns: make(map[int64]*sql.Conn),
	}
}

// Acquire tries to obtain a PostgreSQL session-level advisory lock for key.
// It pins a dedicated connection for the duration so Release can unlock the same session.
func (d *DB) Acquire(ctx context.Context, key int64) (bool, error) {
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

// Release unlocks the advisory lock for key and returns its connection to the pool.
func (d *DB) Release(ctx context.Context, key int64) error {
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

// Notify sends a PostgreSQL NOTIFY on the given channel with payload.
func (d *DB) Notify(ctx context.Context, channel, payload string) error {
	_, err := d.db.ExecContext(ctx, "SELECT pg_notify($1, $2)", channel, payload)
	return err
}

// Listen issues LISTEN on channel and streams incoming notifications until ctx is cancelled.
// The returned channel is closed when the context ends.
func (d *DB) Listen(ctx context.Context, channel string) (<-chan latch.Signal, error) {
	sqlConn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}

	if _, err := sqlConn.ExecContext(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		sqlConn.Close()
		return nil, fmt.Errorf("listen %s: %w", channel, err)
	}

	ch := make(chan latch.Signal, 16)
	go func() {
		defer sqlConn.Close()
		defer close(ch)
		// Raw keeps the connection pinned; the loop runs until ctx is done.
		sqlConn.Raw(func(c any) error {
			pgxConn := c.(*pgxstdlib.Conn).Conn()
			for {
				n, err := pgxConn.WaitForNotification(ctx)
				if err != nil {
					return nil
				}
				select {
				case ch <- latch.Signal{Channel: n.Channel, Payload: n.Payload}:
				case <-ctx.Done():
					return nil
				}
			}
		})
	}()

	return ch, nil
}
