# latch

## Description

`latch` is a Go package that provides distributed coordination primitives — advisory locking and pub/sub signaling — backed by PostgreSQL. It lets multiple processes safely coordinate work and exchange events using a database they already have.

## Usage

### Install

```sh
go get github.com/supazonic/latch
```

### Advisory Locks

Use advisory locks to ensure only one process runs a critical section at a time.

```go
import (
    "context"
    "github.com/supazonic/latch/postgres"
)

db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
l := postgres.New(db)

acquired, err := l.AcquireLock(ctx, 42)
if err != nil {
    // handle error
}
if acquired {
    defer l.ReleaseLock(ctx, 42)
    // only one process reaches here at a time
}
```

### Pub/Sub (LISTEN/NOTIFY)

Send and receive events across processes using PostgreSQL channels.

```go
// Send a notification
err := l.Notify(ctx, "jobs", "payload-here")

// Listen for notifications
handlers := map[latch.Event]latch.Handler{
    "jobs": func(ctx context.Context, payload string) {
        fmt.Println("received:", payload)
    },
}
err := l.Listen(ctx, handlers) // runs in background until ctx is cancelled
```

`Listen` returns immediately and dispatches notifications in a goroutine until the context is cancelled.

### Bring Your Own Implementation

The `latch.Coordinator` interface (combining `Locker` and `Signaler`) lets you swap in any backend:

```go
type Coordinator interface {
    AcquireLock(ctx context.Context, key int64) (bool, error)
    ReleaseLock(ctx context.Context, key int64) error
    Notify(ctx context.Context, channel, payload string) error
    Listen(ctx context.Context, handlers map[Event]Handler) error
}
```
