// Command proplyd runs the proplyd cache server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/c12s/proplyd/pkg/backend/etcd"
	"github.com/c12s/proplyd/pkg/server"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/credentials"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "proplyd:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("proplyd", flag.ExitOnError)
	var (
		listen          = fs.String("listen", env("PROPLYD_LISTEN", ":7070"), "gRPC listen address")
		redisAddrs      = fs.String("redis-addrs", env("PROPLYD_REDIS_ADDRS", "localhost:6379"), "comma-separated Redis addresses (several: cluster, or sentinels with -redis-master)")
		redisMaster     = fs.String("redis-master", env("PROPLYD_REDIS_MASTER", ""), "Redis Sentinel master name")
		redisUser       = fs.String("redis-username", env("PROPLYD_REDIS_USERNAME", ""), "Redis ACL user")
		redisPassword   = fs.String("redis-password", env("PROPLYD_REDIS_PASSWORD", ""), "Redis password")
		redisDB         = fs.Int("redis-db", 0, "Redis database (single node only)")
		redisPrefix     = fs.String("redis-key-prefix", env("PROPLYD_REDIS_KEY_PREFIX", "pd:"), "prefix of every Redis key")
		etcdEndpoints   = fs.String("etcd-endpoints", env("PROPLYD_ETCD_ENDPOINTS", "localhost:2379"), "comma-separated etcd endpoints")
		etcdDialTimeout = fs.Duration("etcd-dial-timeout", 5*time.Second, "etcd dial timeout")
		allowed         = fs.String("allowed-prefixes", env("PROPLYD_ALLOWED_PREFIXES", ""), "comma-separated backend key prefixes namespaces may use (default: any)")
		instanceID      = fs.String("instance-id", env("PROPLYD_INSTANCE_ID", ""), "instance ID for watcher leases (default: hostname)")
		tlsCert         = fs.String("tls-cert", env("PROPLYD_TLS_CERT", ""), "TLS certificate file")
		tlsKey          = fs.String("tls-key", env("PROPLYD_TLS_KEY", ""), "TLS key file")
		logLevel        = fs.String("log-level", env("PROPLYD_LOG_LEVEL", "info"), "debug, info, warn or error")
		maxValue        = fs.Int("max-value-size", 1<<20, "largest value in bytes")
		loadTimeout     = fs.Duration("load-timeout", 5*time.Second, "backend load timeout")
		fenceTTL        = fs.Duration("fence-ttl", 30*time.Second, "fence lifetime (must exceed -load-timeout)")
		leaseTTL        = fs.Duration("lease-ttl", 5*time.Second, "watcher lease duration")
		logSize         = fs.Int64("invalidation-log-size", 10000, "invalidations kept per namespace for resuming subscribers")
		flushInterval   = fs.Duration("flush-interval", 500*time.Millisecond, "write-behind: how long writes accumulate before a flush")
		maxPending      = fs.Int("max-pending", 100000, "write-behind: keys waiting to be flushed per namespace before writes are refused")
		shutdownTimeout = fs.Duration("shutdown-timeout", 30*time.Second, "graceful shutdown timeout")
	)
	_ = fs.Parse(os.Args[1:])

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("log level: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        splitList(*redisAddrs),
		MasterName:   *redisMaster,
		Username:     *redisUser,
		Password:     *redisPassword,
		DB:           *redisDB,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolTimeout:  time.Second,
		MaxRetries:   1,
	})
	defer rdb.Close()
	checkEvictionPolicy(rdb, log)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   splitList(*etcdEndpoints),
		DialTimeout: *etcdDialTimeout,
		Logger:      nil,
	})
	if err != nil {
		return fmt.Errorf("etcd: %w", err)
	}
	defer cli.Close()

	var creds credentials.TransportCredentials
	if *tlsCert != "" || *tlsKey != "" {
		creds, err = credentials.NewServerTLSFromFile(*tlsCert, *tlsKey)
		if err != nil {
			return fmt.Errorf("tls: %w", err)
		}
	}

	srv, err := server.New(server.Options{
		Redis:               rdb,
		Backend:             etcd.New(cli),
		InstanceID:          *instanceID,
		RedisKeyPrefix:      *redisPrefix,
		AllowedPrefixes:     splitList(*allowed),
		MaxValueSize:        *maxValue,
		LoadTimeout:         *loadTimeout,
		FenceTTL:            *fenceTTL,
		LeaseTTL:            *leaseTTL,
		InvalidationLogSize: *logSize,
		FlushInterval:       *flushInterval,
		MaxPending:          *maxPending,
		TLS:                 creds,
		Logger:              log,
	})
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	log.Info("proplyd listening", "addr", lis.Addr().String())

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		return err
	case s := <-sig:
		log.Info("shutting down", "signal", s.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	srv.Shutdown(ctx)
	return nil
}

// checkEvictionPolicy warns about Redis eviction policies that can evict
// namespace metadata (allkeys-*) or fail writes when full (noeviction).
// Entries always have a TTL, metadata never does, so volatile-* policies
// evict only entries.
func checkEvictionPolicy(rdb redis.UniversalClient, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		log.Warn("could not read redis maxmemory-policy", "err", err)
		return
	}
	policy := res["maxmemory-policy"]
	if !strings.HasPrefix(policy, "volatile-") {
		log.Warn("redis maxmemory-policy should be volatile-lru, volatile-lfu or volatile-ttl", "policy", policy)
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
