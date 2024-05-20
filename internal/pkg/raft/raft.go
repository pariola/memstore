package raft

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

var logger = slog.Default()

type peer struct {
	Id      int
	Address string
}

type entry struct {
	Term int
	Data []byte
}

type Role int

const (
	RoleLeader Role = iota + 1
	RoleFollower
	RoleCandidate
)

type Raft struct {
	nodeId int  // node's id
	term   int  // node's current term
	role   Role // node's current role

	peers []peer // neigbours, i'm assumming this does not contain current node

	log []entry // log

	votedFor int // who node has voted for in the current term

	votesReceived map[int]struct{} // if node is a CANDIDATE, tracks which peers it received votes from

	commitedLength int // how much entries from the start of the log has been commited i.e delivered to the application

	leaderNodeId int // leaderNodeId is the id of the current leader, defaults to 0

	leaderLastPing time.Time // last time node heard from the leader

	applyFn func([]byte) // function to pass a message to the application

	// -- Used by Leader

	followerSentLogLength map[int]int // how much log entries has been sent to the follower by the leader

	followerAckedLogLength map[int]int // how much log entries has the follower acknowledge as received

	//

	config           Configuration
	shutdownChan     chan struct{}
	stopElectionChan chan struct{}
}

type Configuration struct {
	Address           string
	ElectionTimeout   time.Duration
	HeartbeatTimeout  time.Duration
	BroadcastInterval time.Duration
}

func New(cfg Configuration) *Raft {
	r := &Raft{
		term: 0,
		role: RoleFollower,

		log: make([]entry, 0),

		followerSentLogLength:  make(map[int]int),
		followerAckedLogLength: make(map[int]int),

		shutdownChan:     make(chan struct{}),
		stopElectionChan: make(chan struct{}, 1), // buffered to avoid blocking
	}

	// start heartbeat timeout
	go r.heartbeatTimeout()

	// start periodic broadcast
	go r.periodicBroadcast()

	return r
}

// heartbeat handles the timeout for the leader to send heartbeats to followers
func (r *Raft) heartbeatTimeout() {
	t := time.NewTicker(r.config.HeartbeatTimeout)

	for {
		select {
		case <-r.shutdownChan:
			t.Stop()
			return

		case <-t.C:
			// only followers should check for leader heartbeats - helps avoid candidate/leader creating elections
			if r.role != RoleFollower {
				continue
			}

			// check if leader has sent a heartbeat within the timeout
			if time.Since(r.leaderLastPing) < r.config.HeartbeatTimeout {
				continue
			}

			r.startElection()
		}
	}
}

// startElection [ALL]
func (r *Raft) startElection() {
	r.term++
	r.role = RoleCandidate

	// vote itself
	r.votedFor = r.nodeId
	r.votesReceived = map[int]struct{}{
		r.nodeId: {},
	}

	request := &VoteRequest{
		Id:   r.nodeId,
		Term: r.term,

		// we include the log's last term & length of the log because we only want leaders who are most current/up-to-date
		LogLength:   len(r.log),
		LogLastTerm: r.getLogLastTerm(),
	}

	// start election timer
	ctx, cancel := context.WithTimeout(context.Background(), r.config.ElectionTimeout)
	defer cancel()

	var wg sync.WaitGroup

	// send vote request to all nodes
	for _, peer := range r.peers {
		wg.Add(1)
		go r.requestPeerVote(&wg, peer, request)
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	// wait
	select {
	case <-done:
	case <-ctx.Done():
	case <-r.shutdownChan:
	case <-r.stopElectionChan:
	}
}

// requestPeerVote [CANDIDATE]
func (r *Raft) requestPeerVote(wg *sync.WaitGroup, peer peer, request *VoteRequest) {
	defer wg.Done()

	var response VoteResponse
	r.sendToPeer(peer.Id, request, &response) // send vote request to peer
	r.handleVoteResponse(response)            // handle response
}

// handleVoteRequest [FOLLOWER]
func (r *Raft) handleVoteRequest(req VoteRequest) VoteResponse {
	if req.Term > r.term { // if candidate term is higher, move foward and become a follower.
		r.term = req.Term
		r.role = RoleFollower
		r.votedFor = 0 // remove exiting vote
	}

	logLastTerm := r.getLogLastTerm()

	// three criterias before voting for candidate
	// 1. log is OK
	logOK := (req.LogLastTerm > logLastTerm) || // check candidate's log is up-to-date with our recipient copy - to avoid an outdated node becoming a leader
		(req.LogLastTerm == logLastTerm && req.LogLength >= len(r.log)) // if candidate's last log term and recipient copy are same, check candidate has as much information as recipient has seen

	// 2. recipient has not voted for anyone else, can vote for the same candidate countless times
	canVote := r.votedFor == 0 || r.votedFor == req.Id

	// 3. recipient's term is same as candidate's

	voteGiven := r.term == req.Term && logOK && canVote
	if voteGiven {
		r.votedFor = req.Id // update recipient's vote
	}

	// reply candidate with vote response
	return VoteResponse{
		Id:    r.nodeId,
		Term:  r.term,
		Given: voteGiven,
	}
}

// handleVoteResponse [LEADER]
func (r *Raft) handleVoteResponse(res VoteResponse) {
	if res.Term > r.term { // if response term is higher, move foward and become a follower.
		r.term = res.Term
		r.role = RoleFollower
		r.votedFor = 0 // remove exiting vote

		return // maybe?
	}

	// to process vote response
	// node must still be a candidate in the correct term (i.e same as term in response - which the vote was for)
	proceed := r.role == RoleCandidate && res.Term == r.term && res.Given
	if !proceed {
		return
	}

	r.votesReceived[res.Id] = struct{}{} // add vote

	quorum := r.quorum() // minimum number of votes required to become leader

	// quorum satisfied?
	if len(r.votesReceived) < quorum {
		return
	}

	// promote
	r.role = RoleLeader
	r.leaderNodeId = r.nodeId

	r.stopElectionChan <- struct{}{} // cancel election timer

	// notify peers
	for _, peer := range r.peers {
		r.followerSentLogLength[peer.Id] = len(r.log) // followers most likely already has up-to-date log as the leader
		r.followerAckedLogLength[peer.Id] = 0
		go r.replicateLogTo(peer.Id)
	}
}

// replicateLogTo [LEADER]
func (r *Raft) replicateLogTo(peerNodeId int) {
	// determine logs that haven't been sent to the peer yet
	sentLogLength := r.followerSentLogLength[peerNodeId]
	entries := r.log[sentLogLength:] // new log entries that haven't been sent yet

	var lastSentLogTerm int
	if sentLogLength > 0 { // if we already sent some logs to the peer already
		lastSentLogTerm = r.log[sentLogLength-1].Term
	}

	request := &LogRequest{
		Id:                r.nodeId,
		Term:              r.term,
		Entries:           entries,
		CommitedLength:    r.commitedLength,
		LastSentLogTerm:   lastSentLogTerm,
		LastSentLogLength: sentLogLength,
	}

	// send
	var response LogResponse
	r.sendToPeer(peerNodeId, request, &response) // send log request to peer
	r.handleLogResponse(response)                // handle response
}

// quorum [ALL]
func (r Raft) getLogLastTerm() int {
	if len(r.log) > 0 {
		return r.log[len(r.log)-1].Term
	}
	return 0
}

// quorum [LEADER]
func (r Raft) quorum() int {
	return (len(r.peers) + 1) / 2 // minimum number of votes required to do something
}

// broadcast [ALL]
func (r *Raft) broadcast(msg []byte) {
	if r.role != RoleLeader {
		// not a leader, redirect to leader
		// maybe can handle on application-transport level?
		r.sendToPeer(r.leaderNodeId, &BroadcastRequest{Data: msg}, nil)
		return
	}

	// append message to log with the current term
	r.log = append(r.log, entry{Term: r.term, Data: msg})

	// acknowledge receiving the message on leader
	r.followerAckedLogLength[r.nodeId] = len(r.log)

	// notify peers
	for _, peer := range r.peers {
		go r.replicateLogTo(peer.Id)
	}
}

// periodicBroadcast [LEADER]
// as a leader periodically replicate logs to followers,
// - doubles as a heartbeat for the followrs
// - incase messages get lost during broadcast, they can be re-delivered.
func (r *Raft) periodicBroadcast() {
	t := time.NewTicker(r.config.BroadcastInterval)

	for {
		select {
		case <-r.shutdownChan:
			t.Stop()
			return

		case <-t.C:
			// only leader nodes should broadcast
			if r.role != RoleLeader {
				continue
			}

			// update peers
			for _, peer := range r.peers {
				go r.replicateLogTo(peer.Id)
			}
		}
	}
}

// handleLogRequest [FOLLOWER]
func (r *Raft) handleLogRequest(req LogRequest) LogResponse {
	if req.Term > r.term { // if from node's term is higher, move foward and become a follower.
		r.term = req.Term
		// r.role = RoleFollower ??
		r.votedFor = 0 // remove exiting vote

		r.stopElectionChan <- struct{}{} // cancel election timer
	}

	r.leaderLastPing = time.Now() // update last time follower heard from leader

	if req.Term == r.term { // if both are on same term, accept from node as leader
		r.role = RoleFollower
		r.leaderNodeId = req.Id
	}

	logOK := (len(r.log) >= req.LastSentLogLength) && // follower's log length is at least as long as what leader knows to have sent (i.e leader can resend log entries)
		(req.LastSentLogLength == 0 || req.LastSentLogTerm == r.log[req.LastSentLogLength-1].Term) // leader assume nothing has been sent prevuiusly or leader's last sent log entry term is same at follower's (some RAFT guarantee that ensures the logs are the same)

	good := req.Term == r.term && logOK // follower still on the correct term and the log is good

	var ackLength int

	if good {
		r.appendLogEntries(req)                              // append entries from leader onto local
		ackLength = req.LastSentLogLength + len(req.Entries) // acknowledge the new length (prefix+suffix) of logs received from leader
	}

	return LogResponse{
		Id:         r.nodeId,
		Term:       r.term,
		AckLength:  ackLength,
		Successful: good, // tells the leader if the follower was able to process the update correctly
	}
}

// appendLogEntries [FOLLOWER]
func (r *Raft) appendLogEntries(req LogRequest) {
	if len(req.Entries) > 0 && len(r.log) > req.LastSentLogLength { // there are new entries and follower has more logs than the leader knows about (i.e an overlap - maybe duplicate log request from leader)
		index := min(len(r.log), req.LastSentLogLength+len(req.Entries)) - 1 // pick the shorter one, basically where the overlap is expected to start from

		if r.log[index].Term != req.Entries[index-req.LastSentLogLength].Term { // checks if the overlapping log's term do not match i.e inconsistency
			r.log = r.log[:req.LastSentLogLength] // truncate everything after the leader's last sent index/length
		}
	}

	if req.LastSentLogLength+len(req.Entries) > len(r.log) { // new entries to add

		// we need to add the only new entries to the follower's log, skip overlapping messages
		for i := len(r.log) - req.LastSentLogLength; i < len(req.Entries); i++ {
			r.log = append(r.log, req.Entries[i])
		}
	}

	if req.CommitedLength > r.commitedLength { // leader has commited more messages, we want to do so as well.
		for i := r.commitedLength; i < req.CommitedLength; i++ {
			r.deliver(r.log[i]) // apply/deliver message to application
		}
		r.commitedLength = req.CommitedLength // update since we commited the messages as well
	}
}

// handleLogResponse [LEADER]
func (r *Raft) handleLogResponse(res LogResponse) {
	if res.Term > r.term { // follower is at a higher term, become a follower
		r.term = res.Term
		r.role = RoleFollower
		r.votedFor = 0 // remove exiting vote

		r.stopElectionChan <- struct{}{} // cancel election timer
		return
	}

	good := res.Term == r.term && r.role == RoleLeader // on same term and still a leader
	if !good {
		return
	}

	if res.Successful && res.AckLength >= r.followerAckedLogLength[res.Id] { // logs were applied succesfully, update and commit
		r.followerSentLogLength[res.Id] = res.AckLength
		r.followerAckedLogLength[res.Id] = res.AckLength

		r.commitLogEntries()
	} else if r.followerSentLogLength[res.Id] > 0 { // not successful and leader assumes to have sent messages to follower i.e likey there is a gap in messages
		// try to resend log entries with 1 more message
		r.followerSentLogLength[res.Id]--
		r.replicateLogTo(res.Id)
	}
}

// commitLogEntries [LEADER]
func (r *Raft) commitLogEntries() {
	for r.commitedLength < len(r.log) {
		var acks int

		// check if all followers have acknoledged the entry at [commitedLength]
		for _, peer := range r.peers {
			if r.followerAckedLogLength[peer.Id] > r.commitedLength {
				acks++
			}
		}

		quorum := r.quorum() // minimum number of acks required to commit log entries
		if acks < quorum {
			break
		}

		r.deliver(r.log[r.commitedLength]) // deliver the entry to the application, followers would commit in the next broadcast
		r.commitedLength++
	}
}

// deliver [ALL]
func (r *Raft) deliver(entry entry) {
	r.applyFn(entry.Data)
}

// Shutdown
func (r *Raft) Shutdown() {
	close(r.shutdownChan)
}
