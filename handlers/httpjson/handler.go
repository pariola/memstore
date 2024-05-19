package httpjson

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/pariola/memstore/internal/pkg/store"
)

type server struct {
	store store.Store
}

func New(store store.Store) *server {
	return &server{store: store}
}

func (c *server) handle(rw http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")[1:]
	if len(parts) != 1 {
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	key := parts[0]

	var handler func(rw http.ResponseWriter, r *http.Request, key string)

	switch strings.ToUpper(r.Method) {
	case "GET":
		handler = c.handleGet

	case "POST":
		handler = c.handleSet

	case "DELETE":
		handler = c.handleDelete

	default:
		rw.WriteHeader(http.StatusNotImplemented)
		rw.Write([]byte("Not Implemented"))
		return
	}

	handler(rw, r, key)
}

func (s *server) handleGet(rw http.ResponseWriter, r *http.Request, key string) {
	ctx := r.Context()

	value, err := s.store.Get(ctx, key)
	switch {
	case errors.Is(err, store.ErrKeyNotFound):
		rw.WriteHeader(http.StatusNotFound)
		return

	case err != nil:
		slog.Error("get key error", "err", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
	rw.Write(value)
}

func (s *server) handleSet(rw http.ResponseWriter, r *http.Request, key string) {
	ctx := r.Context()

	value, err := io.ReadAll(r.Body)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte("unable to read request body"))
		return
	}

	err = s.store.Set(ctx, key, value)
	if err != nil {
		slog.Error("set key error", "err", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
}

func (s *server) handleDelete(rw http.ResponseWriter, r *http.Request, key string) {
	ctx := r.Context()

	err := s.store.Del(ctx, key)
	switch {
	case errors.Is(err, store.ErrKeyNotFound):
		rw.WriteHeader(http.StatusNotFound)
		return

	case err != nil:
		slog.Error("delete key error", "err", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
}

func (s *server) Run(addr string) func() {
	slog.Info("starting HTTP listener", "address", addr)

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle) // handles all paths

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,

		// todo: configurations
	}

	go func() {
		err := srv.ListenAndServe()

		switch {
		case errors.Is(err, http.ErrServerClosed):
			slog.Info("server closed")

		case err != nil:
			slog.Error("cannot start HTTP server", "err", err)
		}
	}()

	return func() { srv.Shutdown(context.TODO()) }
}
