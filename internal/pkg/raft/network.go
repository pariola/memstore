package raft

import (
	"io"
	"log/slog"
	"net"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

type LogRequest struct {
	Id   int
	Term int

	LastSentLogTerm   int
	LastSentLogLength int

	Entries []entry

	CommitedLength int
}

type LogResponse struct {
	Id         int
	Term       int
	AckLength  int
	Successful bool
}

type VoteRequest struct {
	Id          int
	Term        int
	LogLength   int
	LogLastTerm int
}

type VoteResponse struct {
	Id    int
	Term  int
	Given bool
}

type BroadcastRequest struct {
	Data []byte
}

func (LogRequest) Type() commandType       { return LogCommand }
func (VoteRequest) Type() commandType      { return VoteCommand }
func (BroadcastRequest) Type() commandType { return BroadcastCommand }

// sendToPeer [ALL]
func (r *Raft) sendToPeer(peerId int, request Command, response any) error {
	panic("not implemented")
}

func (r *Raft) ListenAndServe() error {
	l, err := net.Listen("tcp", r.config.Address)
	if err != nil {
		return errors.Wrap(err, "start listener")
	}

	defer l.Close()
	logger.Info("RAFT listening", "address", r.config.Address)

	for {
		select {
		case <-r.shutdownChan:
			return nil

		default: // continue below
		}

		conn, err := l.Accept()
		if err != nil {
			return errors.Wrap(err, "accept connection")
		}

		go r.handleConn(conn)
	}
}

func (r *Raft) handleConn(conn net.Conn) {
	defer conn.Close()

	log := logger.With("from", conn.RemoteAddr())

	msg, err := io.ReadAll(conn)
	if err != nil {
		log.Warn("read message error", "err", err)
		return
	}

	var cmd command

	// decode message
	err = msgpack.Unmarshal(msg, &cmd)
	if err != nil {
		log.Warn("decode message error", "err", err)
		return
	}

	log = log.With("command", cmd.Type)

	// handle message
	response, err := r.handleCommand(log, cmd)
	if err != nil {
		log.Warn("handle command error", "err", err)
		return
	}

	// encode response
	msg, err = msgpack.Marshal(response)
	if err != nil {
		log.Warn("encode response error", "err", err)
		return
	}

	// send response
	_, err = conn.Write(msg)
	if err != nil {
		log.Warn("write response error", "err", err)
	}
}

func (r *Raft) handleCommand(logger *slog.Logger, cmd command) (any, error) {
	var err error

	switch cmd.Type {

	case LogCommand:
		var request LogRequest
		if err = cmd.unwrap(&request); err != nil {
			return nil, errors.Wrap(err, "decode log request")
		}
		return r.handleLogRequest(request), nil

	case VoteCommand:
		var request VoteRequest
		if err = cmd.unwrap(&request); err != nil {
			return nil, errors.Wrap(err, "decode vote request")
		}
		return r.handleVoteRequest(request), nil

	case BroadcastCommand:
		var request BroadcastRequest
		if err = cmd.unwrap(&request); err != nil {
			return nil, errors.Wrap(err, "decode broadcast request")
		}
		r.broadcast(request.Data)

	default:
		logger.Warn("unknown command", "type", cmd.Type)
	}

	return nil, nil
}
