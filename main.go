package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/pariola/memstore/handlers/httpjson"
	"github.com/pariola/memstore/internal/pkg/store"
)

func main() {
	// load configuration.
	httpAddress := flag.String("addr", ":8080", "HTTP address to listen on")

	// todo: load initial state from disk
	store := store.Basic()

	// start HTTP listener.
	shutdown := httpjson.New(store).Run(*httpAddress)

	// setup gracefull shutdown.
	s := make(chan os.Signal)
	signal.Notify(s, os.Interrupt, os.Kill)
	<-s
	shutdown()
}
