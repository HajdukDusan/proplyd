// Package proplydtest provides test infrastructure for proplyd and its
// clients: a real Redis process that can be killed and restarted, a TCP
// proxy that injects network faults, and restartable proplyd nodes.
package proplydtest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// TB is the part of testing.TB the kit needs. Tests pass their *testing.T;
// programs such as the proplyd-go playground provide their own.
type TB interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Skip(args ...any)
	Cleanup(func())
	TempDir() string
}

// Persistence selects how a test Redis persists data across restarts.
type Persistence int

const (
	// NoPersistence: a restarted Redis is empty.
	NoPersistence Persistence = iota
	// AOF: every write is fsynced; a restarted Redis has all data.
	AOF
	// RDB: only explicit Save snapshots survive; a restarted Redis rolls
	// back to the last snapshot.
	RDB
)

// Redis is a redis-server process owned by a test.
type Redis struct {
	t           TB
	bin         string
	port        int
	dir         string
	persistence Persistence

	mu  sync.Mutex
	cmd *exec.Cmd
}

// StartRedis starts redis-server on a free port. The test is skipped if
// redis-server is not installed (override the path with
// PROPLYD_REDIS_SERVER).
func StartRedis(t TB, p Persistence) *Redis {
	t.Helper()
	bin := os.Getenv("PROPLYD_REDIS_SERVER")
	if bin == "" {
		var err error
		if bin, err = exec.LookPath("redis-server"); err != nil {
			t.Skip("redis-server not found; install Redis or set PROPLYD_REDIS_SERVER")
		}
	}
	r := &Redis{t: t, bin: bin, port: freePort(t), dir: t.TempDir(), persistence: p}
	r.Start()
	t.Cleanup(r.Kill)
	return r
}

// Addr returns host:port.
func (r *Redis) Addr() string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(r.port)) }

// Start starts the process (again) on the same port.
func (r *Redis) Start() {
	r.t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cmd != nil {
		return
	}
	args := []string{
		"--port", strconv.Itoa(r.port), "--bind", "127.0.0.1",
		"--dir", r.dir, "--protected-mode", "no", "--loglevel", "warning",
		"--maxmemory-policy", "volatile-lru", "--save", "",
	}
	switch r.persistence {
	case AOF:
		args = append(args, "--appendonly", "yes", "--appendfsync", "always")
	default:
		args = append(args, "--appendonly", "no")
	}
	cmd := exec.Command(r.bin, args...)
	if err := cmd.Start(); err != nil {
		r.t.Fatalf("starting redis-server: %v", err)
	}
	r.cmd = cmd
	cli := redis.NewClient(&redis.Options{Addr: r.Addr(), MaxRetries: -1})
	defer cli.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		err := cli.Ping(ctx).Err()
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("redis-server did not start: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Kill stops the process with SIGKILL.
func (r *Redis) Kill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cmd == nil {
		return
	}
	_ = r.cmd.Process.Kill()
	_ = r.cmd.Wait()
	r.cmd = nil
}

// Restart kills and starts the process.
func (r *Redis) Restart() {
	r.t.Helper()
	r.Kill()
	r.Start()
}

// Save writes an RDB snapshot (for RDB persistence).
func (r *Redis) Save() {
	r.t.Helper()
	cli := r.Client()
	defer cli.Close()
	if err := cli.Save(context.Background()).Err(); err != nil {
		r.t.Fatalf("SAVE: %v", err)
	}
}

// Client returns a new client for assertions. The caller closes it.
func (r *Redis) Client() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: r.Addr()})
}

// NewRedisClient returns a client with the short timeouts proplyd uses, so
// that outages are detected quickly in tests.
func NewRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  300 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		PoolTimeout:  500 * time.Millisecond,
		MaxRetries:   -1,
	})
}

func freePort(t TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Eventually retries cond until it returns nil or timeout passes.
func Eventually(t TB, timeout time.Duration, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met after %v: %v", timeout, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Errorf is a convenience for Eventually conditions.
func Errorf(format string, args ...any) error { return fmt.Errorf(format, args...) }

func init() {
	// go-redis logs every failed dial; tests kill Redis on purpose.
	if _, ok := os.LookupEnv("PROPLYD_TEST_LOG"); !ok {
		redis.SetLogger(silentLogger{})
	}
}

type silentLogger struct{}

func (silentLogger) Printf(context.Context, string, ...any) {}
