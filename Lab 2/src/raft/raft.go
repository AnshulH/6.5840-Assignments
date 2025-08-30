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

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	electionState string
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	log []string
	commitIndex int
	lastApplied int
	nextApplied []int
	matchIndex []int
	electionTicker *time.Ticker
	heartbeatTicker *time.Ticker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = rf.electionState == "Leader"
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
    defer rf.mu.Unlock()
    
    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    
    // If candidate's term is older, reject
    if args.Term < rf.currentTerm {
        return
    }
    
    // If candidate's term is newer, update our term and reset vote
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.electionState = "Follower"
    }
    
    // Grant vote if we haven't voted or already voted for this candidate
    if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true
        reply.Term = rf.currentTerm
        
        // Reset election timer when granting vote
        ms := getRandTimeVal()
        rf.electionTicker.Reset(time.Duration(ms) * time.Millisecond)
    }
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []int
	LeaderCommit int
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || rf.electionState == "Leader" {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if (rf.electionState == "Candidate" || rf.electionState == "Follower") && rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.electionState = "Follower"
		reply.Success = true
		reply.Term = args.Term
		ms := getRandTimeVal()
		rf.electionTicker.Reset(time.Duration(ms) * time.Millisecond)
		return 
	}
}


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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
    
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func getRandTimeVal() int64 {
	return 50 + (rand.Int63() % 300);
}

func (rf *Raft) sendReqVoteWg(server int, ch chan RequestVoteReply, wg *sync.WaitGroup) {
	reply := RequestVoteReply{}

	reqVotes := RequestVoteArgs{}
	reqVotes.Term = rf.currentTerm
	reqVotes.CandidateId = rf.me

	rf.sendRequestVote(server, &reqVotes, &reply);
	ch <- reply

	wg.Done()
}

func (rf *Raft) sendHeartbeatWg(server int, ch chan AppendEntriesReply, wg *sync.WaitGroup) {
	reply := AppendEntriesReply{}

	hb := AppendEntriesArgs{}
	hb.Term = rf.currentTerm
	hb.LeaderId = rf.me

	rf.sendAppendEntries(server, &hb, &reply);
	ch <- reply

	wg.Done()
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.electionState != "Leader" {
		rf.heartbeatTicker.Stop()
		return
	}

	ch := make(chan AppendEntriesReply, len(rf.peers) - 1)

	wg := sync.WaitGroup{}

	for idx, _ := range rf.peers  {
		if (idx == rf.me)  {
			continue
		}
        wg.Add(1)

        //now we spawn a goroutine
        go rf.sendHeartbeatWg(idx, ch, &wg);
    }

    wg.Wait()

    close(ch)

	ms := getRandTimeVal()
	rf.heartbeatTicker.Reset(time.Duration(50) * time.Millisecond)
	rf.electionTicker.Reset(time.Duration(ms) * time.Millisecond)
	return
}	

func (rf *Raft) startElectionAttempt() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.electionState == "Leader" {
		return
	}

	if (rf.electionState == "Follower" || rf.electionState == "Candidate") {
		rf.electionState = "Candidate"
		rf.currentTerm += 1
	}

	totalVotes := 1
	rf.votedFor = rf.me

	ch := make(chan RequestVoteReply, len(rf.peers) - 1)

	wg := sync.WaitGroup{}

	for idx, _ := range rf.peers  {
		if (idx == rf.me)  {
			continue
		}
        wg.Add(1)

        //now we spawn a goroutine
        go rf.sendReqVoteWg(idx, ch, &wg);
    }

    // now we wait for everyone to finish - again, not a must.
    // you can just receive from the channel N times, and use a timeout or something for safety
    wg.Wait()

    // we need to close the channel or the following loop will get stuck
    close(ch)

	for val := range ch {
		if val.VoteGranted && val.Term == rf.currentTerm {
			totalVotes += 1;
		}
	}

	if totalVotes >= len(rf.peers) / 2 {
		rf.electionState = "Leader"
		rf.heartbeatTicker = time.NewTicker(time.Duration(50) * time.Millisecond)
		return
	}

	rf.electionState = "Follower"
	ms := getRandTimeVal()
	rf.electionTicker.Reset(time.Duration(ms) * time.Millisecond)
	return
}

func (rf *Raft) ticker() {
	if rf.electionTicker != nil {
        defer rf.electionTicker.Stop()
    }
    if rf.heartbeatTicker != nil {
        defer rf.heartbeatTicker.Stop()
    }
	for rf.killed() == false {
		select {
		case <- rf.electionTicker.C: 
			go rf.startElectionAttempt()
		case <- rf.heartbeatTicker.C:
			go rf.sendHeartbeats()
		}
	}		
}

// We need to rewrite with tickers instead of trying to manage states with normal for loops.
// func (t *time.Ticker) resetTicker() {
//     t.ticker = *time.NewTicker(t.period)
// }

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]string, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionState = "Follower"

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	ms := getRandTimeVal()
	rf.electionTicker = time.NewTicker(time.Duration(ms) * time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(time.Duration(ms) * time.Millisecond)
	go rf.ticker()

	return rf
}
