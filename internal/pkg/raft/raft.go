package raft

import "time"

type RequestType int

const (
	VoteRequest RequestType = iota + 1
	LogRequest
)

type LogRequestData struct {
	Id   int
	Term int

	LastSentLogTerm   int
	LastSentLogLength int

	Entries []logEntry

	CommitedLength int
}

type LogResponseData struct {
	Id         int
	Term       int
	AckLength  int
	Successful bool
}

type VoteRequestData struct {
	Id          int
	Term        int
	LogLength   int
	LogLastTerm int
}

type VoteResponseData struct {
	Id    int
	Term  int
	Given bool
}

type peer struct {
	Id      int
	Address string

	// health info
}

type logEntry struct {
	Data int
	Term int
}

type Role int

const (
	RoleLeader Role = iota + 1
	RoleFollower
	RoleCandidate
)

type Raft struct {
	ticker time.Ticker

	nodeId int

	term int
	role Role

	votedFor      int
	votesReceived map[int]struct{}

	log []logEntry

	peers []peer // neigbours, i'm assumming this does not contain current node

	leaderNodeId int

	// -- Used by all

	// how much entries from the start of the log has been commited i.e delivered to the application
	commitedLength int

	// -- Used by Leader

	// how much log entries has the follower acknowledge as received
	followerAckedLogLength map[int]int

	// how much log entries has been sent to the follower by the leader
	followerSentLogLength map[int]int

	apply func(logEntry)
}

func (r *Raft) startElection() {
	r.term++
	r.role = RoleCandidate

	// vote itself
	r.votedFor = r.nodeId
	r.votesReceived = map[int]struct{}{
		r.nodeId: {},
	}

	request := &VoteRequestData{
		Id:   r.nodeId,
		Term: r.term,

		// we include the log's last term & length of the log because we only want leaders who are most current/up-to-date
		LogLength:   len(r.log),
		LogLastTerm: r.getLogLastTerm(),
	}

	// send vote request to all nodes
	for _, peer := range r.peers {
		r.sendToPeer(peer.Id, VoteRequest, request)
	}

	// start timer waiting for replies
}

func (r *Raft) handleVoteRequest(req VoteRequestData) VoteResponseData {
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
	return VoteResponseData{
		Id:    r.nodeId,
		Term:  r.term,
		Given: voteGiven,
	}
}

func (r *Raft) handleVoteResponse(res VoteResponseData) {
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

	quorum := (len(r.peers) + 1) / 2 // minimum number of votes required to become leader

	// check quorum satisfied?
	if len(r.votesReceived) < quorum {
		return
	}

	// promote
	r.role = RoleLeader
	r.leaderNodeId = r.nodeId

	// todo: cancel election timer

	// notify peers
	for _, peer := range r.peers {
		r.followerSentLogLength[peer.Id] = len(r.log) // followers most likely already has up-to-date log as the leader
		r.followerAckedLogLength[peer.Id] = 0
		r.replicateLogTo(peer.Id)
	}
}

func (r *Raft) replicateLogTo(peerNodeId int) {
	// determine logs that haven't been sent to the peer yet
	sentLogLength := r.followerSentLogLength[peerNodeId]
	entries := r.log[sentLogLength:] // new log entries that haven't been sent yet

	var lastSentLogTerm int
	if sentLogLength > 0 { // if we already sent some logs to the peer already
		lastSentLogTerm = r.log[sentLogLength-1].Term
	}

	data := LogRequestData{
		Id:                r.nodeId,
		Term:              r.term,
		Entries:           entries,
		LastSentLogLength: sentLogLength,
		LastSentLogTerm:   lastSentLogTerm,

		CommitedLength: 0, // todo: wth?
	}

	// send
	r.sendToPeer(peerNodeId, LogRequest, data)
}

func (r Raft) getLogLastTerm() int {
	if len(r.log) > 0 {
		return r.log[len(r.log)-1].Term
	}
	return 0
}

func (r *Raft) sendToPeer(peerId int, requestType RequestType, data any) {
	panic("not implemented")
}

func (r *Raft) broadcast(msg []byte) {
	if r.role != RoleLeader {
		// todo: redirect/forward to leader
		return
	}

	// append message to log with the current term

	// acknowledge receiving the message on leader
	r.followerAckedLogLength[r.nodeId] = len(r.log)

	// notify peers
	for _, peer := range r.peers {
		r.replicateLogTo(peer.Id)
	}
}

func (r *Raft) periodicBroadcast() {
	// as a leader periodically replicate logs to followers,
	// - doubles as a heartbeat for the followrs
	// - incase messages get lost during broadcast, they can be re-delivered.

	for range r.ticker.C {
		if r.role != RoleLeader {
			continue
		}

		// update peers
		for _, peer := range r.peers {
			r.replicateLogTo(peer.Id)
		}
	}
}

func (r *Raft) handleLogRequest(req LogRequestData) LogResponseData {
	if req.Term > r.term { // if from node's term is higher, move foward and become a follower.
		r.term = req.Term
		// r.role = RoleFollower ??
		r.votedFor = 0 // remove exiting vote

		// todo: cancel election timer
	}

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

	return LogResponseData{
		Id:         r.nodeId,
		Term:       r.term,
		AckLength:  ackLength,
		Successful: good, // tells the leader if the follower was able to process the update correctly
	}
}

func (r *Raft) appendLogEntries(req LogRequestData) {
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
			r.apply(r.log[i]) // apply/deliver message to application
		}
		r.commitedLength = req.CommitedLength // update since we commited the messages as well
	}
}
