// Command proplyd runs the centralized c12s cache server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/c12s/proplyd/backend/etcd"
	"github.com/c12s/proplyd/cache"
	"github.com/c12s/proplyd/server"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "proplyd:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen          = flag.String("listen", env("PROPLYD_LISTEN", ":7070"), "gRPC listen address")
		endpoints       = flag.String("etcd-endpoints", env("PROPLYD_ETCD_ENDPOINTS", "localhost:2379"), "comma separated etcd endpoints")
		dialTimeout     = flag.Duration("etcd-dial-timeout", 5*time.Second, "etcd dial timeout")
		loadTimeout     = flag.Duration("load-timeout", 5*time.Second, "timeout of a single read-through load")
		shutdownTimeout = flag.Duration("shutdown-timeout", 30*time.Second, "time allowed to drain requests and flush write-behind data")
		allowed         = flag.String("allowed-prefixes", env("PROPLYD_ALLOWED_PREFIXES", ""), "comma separated key prefixes namespaces may use (empty: any)")
		logLevel        = flag.String("log-level", env("PROPLYD_LOG_LEVEL", "info"), "debug, info, warn or error")
		tlsCert         = flag.String("tls-cert", env("PROPLYD_TLS_CERT", ""), "server TLS certificate (PEM); enables TLS together with -tls-key")
		tlsKey          = flag.String("tls-key", env("PROPLYD_TLS_KEY", ""), "server TLS private key (PEM)")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*endpoints, ","),
		DialTimeout: *dialTimeout,
	})
	if err != nil {
		return fmt.Errorf("etcd: %w", err)
	}
	defer cli.Close()

	engine, err := cache.New(cache.Options{
		Backends:     etcd.NewFactory(cli),
		Logger:       log,
		LoadTimeout:  *loadTimeout,
		PrefixPolicy: prefixPolicy(*allowed),
	})
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	opts := server.DefaultServerOptions()
	if *tlsCert != "" || *tlsKey != "" {
		creds, err := credentials.NewServerTLSFromFile(*tlsCert, *tlsKey)
		if err != nil {
			return fmt.Errorf("tls: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	srv := server.Start(lis, engine, log, opts...)
	log.Info("proplyd started", "listen", lis.Addr().String(), "etcd", *endpoints, "epoch", engine.Epoch())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Info("shutting down", "timeout", *shutdownTimeout)
	sctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	return srv.Shutdown(sctx)
}

func prefixPolicy(allowed string) func(string, string) error {
	if allowed == "" {
		return nil
	}
	prefixes := strings.Split(allowed, ",")
	return func(_, prefix string) error {
		for _, p := range prefixes {
			if strings.HasPrefix(prefix, p) {
				return nil
			}
		}
		return errors.New("prefix not allowed")
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
