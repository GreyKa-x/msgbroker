package main

import (
	"context"
	"github.com/GreyKa-x/msgbroker/internal/broker"
	"github.com/GreyKa-x/msgbroker/internal/web"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		addr := os.Getenv("LISTEN_ADDR")
		if addr == "" {
			addr = ":8000"
		}
		if strings.HasPrefix(addr, ":") {
			addr = "127.0.0.1" + addr
		}
		client := http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get("http://" + addr + "/healthz")
		if err != nil {
			os.Exit(1)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	workers, err := broker.ParseDeliveryWorkers(os.Getenv("DELIVERY_WORKERS"))
	if err != nil {
		return err
	}
	dsn, err := broker.DatabaseDSN(os.Getenv("MYSQL_DSN"), os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	store, err := broker.Open(dsn)
	if err != nil {
		return err
	}
	defer store.DB.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err = store.Migrate(ctx); err != nil {
		return err
	}
	engine := broker.NewEngine(store)
	engine.Workers = workers
	server, err := web.New(store, engine, os.Getenv("AGENT_API_TOKEN"))
	if err != nil {
		return err
	}
	done := make(chan struct{})
	if os.Getenv("DISPATCH_ENABLED") != "false" {
		if err = store.BootstrapJobs(ctx); err != nil {
			return err
		}
		go func() { defer close(done); engine.Run(ctx) }()
	} else {
		close(done)
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8000"
	}
	httpServer := &http.Server{Addr: addr, Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		httpServer.Shutdown(shutdown)
	}()
	err = httpServer.ListenAndServe()
	cancel()
	<-done
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
