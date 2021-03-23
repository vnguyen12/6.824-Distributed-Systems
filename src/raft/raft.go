package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	NullVote             = -1
	MinElectionTimeout   = 300
	ElectionTimeOutRandN = 200 //between 100 - 500 ms
	HeartbeatTimeout     = 100 * time.Millisecond
	Follower             = 0
	Candidate            = 1
	Leader               = 2
	MaxChanSize          = 100
)

//
// ApplyMsg
// -------------
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry
//
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

/**************************************************
 * Raft struct
 * A Go object implementing a single Raft peer.
 **************************************************/
type Raft struct {
	mux       sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//Other fields
	state               int
	voteCount           int
	commitChan          chan bool
	leaderHeartbeatChan chan bool
	grantVoteChan       chan bool
	wonElectionChan     chan bool
}

/**************************************************
 * GetState()
 * return currentTerm and whether this server
 * believes it is the leader.
 **************************************************/
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mux.Lock()
	defer rf.mux.Unlock()

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// TODO
// -----------------------------------------
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// TODO
// -----------------------------------------
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// TODO
// -----------------------------------------
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// TODO
// -----------------------------------------
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

/************************************
 * Request Vote RPC
 ************************************/
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mux.Lock()
	defer rf.mux.Unlock()

	// Reply false if term < currentTerm (§5.1)
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = NullVote
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log,
	// grant vote (§5.2, §5.4)
	voteForCand := false
	if rf.votedFor == NullVote || rf.votedFor == args.CandidateID {
		voteForCand = true
	}
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date
	// If the logs end with the same term, then whichever log is longer is more up-to-date (§5.4)
	candLogUpToDate := false
	if term < args.LastLogTerm || (term == args.LastLogTerm && index <= args.LastLogIndex) {
		candLogUpToDate = true
	}

	if voteForCand && candLogUpToDate {
		rf.grantVoteChan <- true
		rf.state = Follower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Starts an election for each candidate
func (rf *Raft) runElection() {
	rf.mux.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	state := rf.state
	rf.mux.Unlock()

	for i := range rf.peers {
		if i != rf.me && state == Candidate {
			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, reply)
				if ok {
					rf.processVoteReply(&args, reply)
				}
			}(i)
		}
	}
}

// Processes the votes returned by the RequestVote RPC call for each candidate
func (rf *Raft) processVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	term := rf.currentTerm

	if rf.state != Candidate {
		return
	}
	if args.Term != term {
		return
	}
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > term {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = NullVote
	}

	// If received the majority of votes, candidate becomes the new leader
	if reply.VoteGranted {
		rf.voteCount++
		if rf.state == Candidate && rf.voteCount > len(rf.peers)/2 {
			rf.wonElectionChan <- true
		}
	}
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mux.Lock()
	defer rf.mux.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == Leader
	if isLeader {
		index = rf.getLastIndex() + 1
		entry := LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		}
		// If command received from client: append entry to local log, respond after entry applied to state machine
		// (§5.3)
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	}

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// rf := &Raft{}
	// rf.peers = peers
	// rf.persister = persister
	// rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf := &Raft{
		//persistent states on all servers
		peers:       peers,
		persister:   persister,
		me:          me,
		currentTerm: 0,
		votedFor:    NullVote,
		log:         []LogEntry{{Term: 0}}, // Skip index 0

		// volatile state of all servers
		commitIndex: 0,
		lastApplied: 0,

		// volatile state of leader
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		// Other Fields
		state:               Follower,
		voteCount:           0,
		leaderHeartbeatChan: make(chan bool, MaxChanSize),
		commitChan:          make(chan bool, MaxChanSize),
		grantVoteChan:       make(chan bool, MaxChanSize),
		wonElectionChan:     make(chan bool, MaxChanSize),
	}

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.NotifyApplyCh(applyCh)

	return rf
}

// NotifyApplyCh
// Notify the tester whenever a follower makes a commit
func (rf *Raft) NotifyApplyCh(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.commitChan:
			rf.mux.Lock()
			//If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
			baseIndex := rf.log[0].Index
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandIndex: i,
					Command:      rf.log[i-baseIndex].Command,
					CommandValid: true,
				}
				applyCh <- msg
				rf.lastApplied = i
			}
			rf.mux.Unlock()
		}
	}
}

/************************************************************************************
 * The main ticker go routine starts a new election if this peer hasn't received
 * heartsbeats recently.
 * Checks and executes functions based on the state of each Raft peer.
 ************************************************************************************/
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mux.Lock()
		state := rf.state
		rf.mux.Unlock()

		switch state {
		case Follower:
			// Receives heartbeats from leader and issues votes
			// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote
			// to candidate: convert to candidate
			rf.mux.Lock()
			timeout := rf.randElectionTimeout()
			rf.mux.Unlock()

			select {
			case <-rf.leaderHeartbeatChan:
			case <-rf.grantVoteChan:
			case <-time.After(timeout):
				rf.becomeCandidate()
			}

		case Leader:
			// Broadcast AppendEntries and heartbeats to followers after timeout
			rf.broadcastAppend()
			time.Sleep(HeartbeatTimeout)

		case Candidate:
			// On conversion to candidate, start election:
			//	• Increment currentTerm
			//	• Vote for self
			//	• Reset election timer
			//	• Send RequestVote RPCs to all other servers
			//	• If votes received from majority of servers: become leader
			//	• If AppendEntries RPC received from new leader: convert to
			//	follower
			//	• If election timeout elapses: start new election
			rf.mux.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			timeout := rf.randElectionTimeout()
			rf.mux.Unlock()

			go rf.runElection()
			select {
			case <-time.After(timeout):
			case <-rf.leaderHeartbeatChan:
				rf.becomeFollower()
			case <-rf.wonElectionChan:
				rf.becomeLeader()
			}
		}
	}
}

/******************************************
* AppendEntries RPC
*******************************************/

// AppendEntriesArgs: Sent by leader to peers for heartbeat or log update
type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderID     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply: Replies from peer servers
type AppendEntriesReply struct {
	// Your data here.
	Term      int
	Success   bool
	NextIndex int
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	firstIndex := rf.log[0].Index

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.voteCount = NullVote
	}

	rf.leaderHeartbeatChan <- true

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry
	// and all that follow it (§5.3)
	if args.PrevLogIndex > firstIndex {
		term := rf.log[args.PrevLogIndex-firstIndex].Term
		if args.PrevLogTerm != term {
			for index := args.PrevLogIndex - 1; index >= firstIndex; index-- {
				if rf.log[index-firstIndex].Term != term {
					reply.NextIndex = index + 1
					break
				}
			}
			return
		}
	}

	// Append any new entries not already in the log
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.NextIndex = rf.getLastIndex() + 1
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.state = Follower

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last log entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = getMin(args.LeaderCommit, rf.getLastIndex())
		rf.commitChan <- true
	}
}

// Send an AppendEntry RPC to a peer
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Called by the leader to broadcast log entries and heartbeats to followers
func (rf *Raft) broadcastAppend() {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	N := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].Index

	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].Term == rf.currentTerm {
				num++
			}
		}
		if num > len(rf.peers)/2 {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.commitChan <- true
	}

	for peer := range rf.peers {
		if peer != rf.me && rf.state == Leader {

			if rf.nextIndex[peer] > baseIndex {
				prevLogIndex := rf.nextIndex[peer] - 1
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[prevLogIndex-baseIndex].Term,
					Entries:      make([]LogEntry, len(rf.log[prevLogIndex+1-baseIndex:])),
					LeaderCommit: rf.commitIndex,
				}

				// Send log entries for followers to replicate or if up-to-date, then serves as a heartbeat
				copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])

				go func(peer int, args AppendEntriesArgs) {
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(peer, &args, reply)
					if ok {
						rf.processAppendReply(peer, &args, reply)
					}
				}(peer, args)
			}
		}
	}
}

// Called by the leader to process the replies from the AppendReply RPC calls
func (rf *Raft) processAppendReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	if rf.state != Leader {
		return
	}
	if args.Term != rf.currentTerm {
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = NullVote
		return
	}

	// If successful: update nextIndex and matchIndex for follower (§5.3)
	// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[peer] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		}
	} else {
		rf.nextIndex[peer] = reply.NextIndex
	}
}

/***************************
* Helper functions
****************************/
func (rf *Raft) randElectionTimeout() time.Duration {
	rand.Seed(int64(rf.me) + time.Now().UnixNano())
	randTime := rand.Intn(ElectionTimeOutRandN)
	randElectionTimeOut := time.Duration(MinElectionTimeout+randTime) * time.Millisecond
	return randElectionTimeOut
}

func (rf *Raft) becomeCandidate() {
	rf.mux.Lock()
	rf.state = Candidate
	rf.mux.Unlock()
}

func (rf *Raft) becomeFollower() {
	rf.mux.Lock()
	rf.state = Follower
	rf.mux.Unlock()
}

func (rf *Raft) becomeLeader() {
	rf.mux.Lock()
	rf.state = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.mux.Unlock()
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func getMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}
