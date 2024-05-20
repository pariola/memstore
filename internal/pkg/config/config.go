package config

import (
	"encoding/json"
	"os"
)

type Node struct {
	Peers  []string `json:"peers"`
	Listen string   `json:"listen"`
}

type Raft struct {
	Nodes               map[string]Node `json:"nodes"`
	ElectionTimeout     int             `json:"election_timeout"`
	BroadcastInterval   int             `json:"broadcast_interval"`
	MaxHeartbeatTimeout int             `json:"max_heartbeat_timeout"`
}

type Configuration struct {
	Raft   Raft   `json:"raft"`
	Listen string `json:"listen"`
}

func Load(path string) (Configuration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Configuration{}, err
	}

	var cfg Configuration
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return Configuration{}, err
	}

	return cfg, nil
}
