package raft

import "github.com/vmihailenco/msgpack/v5"

type commandType int

const (
	LogCommand commandType = iota
	VoteCommand
	BroadcastCommand
)

type Command interface {
	Type() commandType
}

type command struct {
	Type commandType
	Data msgpack.RawMessage
}

func newCommand(src Command) (command, error) {
	data, err := msgpack.Marshal(src)
	if err != nil {
		return command{}, err
	}
	return command{Type: src.Type(), Data: data}, nil
}

func (c command) unwrap(dst any) error {
	return msgpack.Unmarshal(c.Data, dst)
}
