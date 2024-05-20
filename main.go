package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"

	"github.com/pariola/memstore/handlers/httpjson"
	"github.com/pariola/memstore/internal/pkg/config"
	"github.com/pariola/memstore/internal/pkg/store"
)

func main() {
	nodeName := flag.String("node", "", "configuration node to use")
	configFilePath := flag.String("configFile", "./config.json", "configuration file path")
	flag.Parse()

	if *nodeName == "" {
		panic("node to run as not specified")
	}

	// load configuration
	cfg, err := config.Load(*configFilePath)
	if err != nil {
		panic(err)
	}

	// verify node exists
	_, ok := cfg.Raft.Nodes[*nodeName]
	if !ok {
		panic("node does not exist in configuration file")
	}

	slog.Info("starting node", "name", *nodeName)

	// todo: load initial state from disk
	store := store.Basic()

	// start Raft.

	// start HTTP listener.
	shutdown := httpjson.New(store).Run(cfg.Listen)

	// setup gracefull shutdown.
	s := make(chan os.Signal)
	signal.Notify(s, os.Interrupt, os.Kill)
	<-s
	shutdown()
}
