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

import "sync"
import (
	"labrpc"
	//"syscall"
	"fmt"
	"time"
	"math/rand"
	"math"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Log struct {
	Term	int
	Command	interface{}
}

type Raft struct {
	mu        	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]

	// Persist state on all servers
	currentTerm 	int
	votedFor 	int
	log 		[]Log
	lastIndex	int

	//Volatile state on all servers
	commitIndex 	int
	lastApplied 	int
	state		int // Followers, Candidates or Leaders
	timer		* time.Timer

	//Volatile state on leaders
	nextIndex 	[]int
	matchIndex 	[]int
	leaderTimer	* time.Timer


	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == 2
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term		int
	CandidateId	int
	LastLogIndex	int
	LastLogTerm	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("#%d server receive RequestVote rpc from #%d at term %d\n", rf.me, args.CandidateId, args.Term)
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm != rf.log[rf.lastIndex].Term {
			reply.VoteGranted = (args.LastLogTerm > rf.log[rf.lastIndex].Term)
		} else {
			reply.VoteGranted = (args.LastLogIndex >= rf.lastIndex)
		}
	} else {
		reply.VoteGranted = false
	}
	if (reply.VoteGranted) {
		rf.votedFor = args.CandidateId
		var d time.Duration
		d = time.Duration(2000 * rand.Float64() + 1000)
		rf.timer.Reset(d * time.Millisecond)
	}
	fmt.Printf("#%d server receive RequestVote rpc from #%d, result: %v\n", rf.me, args.CandidateId, reply.VoteGranted)
	return
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
// is no need to implement your own timeouts aro.und Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("#%d server send requestvote rpc to #%d at term %d\n", args.CandidateId, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term		int // leader's term
	LeaderId	int
	PrevLogIndex	int // index of log entry immediately preceding new ones
	PrevLogTerm	int // term of log entry immediately preceding new ones
	Entries		[]Log // new entries
	LeaderCommit	int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term 		int // currentTerm, for leader to update itself
	Success 	bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("#%d server send appendentries rpc to #%d at term %d\n", args.LeaderId, server, args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("#%d server receive appendentries rpc from #%d at term %d\n", rf.me, args.LeaderId, args.Term)
	// Your code here (2A, 2B).
	var d time.Duration
	d = time.Duration(2000 * rand.Float64() + 1000)
	if args.Term < rf.currentTerm { // expired rpc
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	rf.state = 0
	rf.timer.Reset(d * time.Millisecond)
	if !(rf.lastIndex >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	entryPos := 0
	for i := args.PrevLogIndex + 1; i <= rf.lastIndex; i ++ {
		entryPos = i - args.PrevLogIndex - 1
		if i >= len(args.Entries) {
			break
		}
		if rf.log[i].Term != args.Entries[entryPos].Term {
			rf.log = rf.log[: i]
			rf.lastIndex = len(rf.log) - 1
			break
		}
	}
	c := make([]Log, len(rf.log) + len(args.Entries[entryPos :]))
	copy(c, rf.log)
	copy(c[len(rf.log):], args.Entries[entryPos :])
	rf.log = c
	rf.lastIndex = len(rf.log) - 1
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.lastIndex)))
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) Heartbeat() {
	for _, v := rf.GetState(); v; {
		rf.leaderTimer = time.NewTimer(100 * time.Millisecond)
		args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.lastIndex, PrevLogTerm: rf.log[rf.lastIndex].Term, LeaderCommit: rf.commitIndex}
		reply := &AppendEntriesReply{}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if (rf.sendAppendEntries(i, args, reply)) {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
				}
			}
		}
		<- rf.leaderTimer.C
	}
}

func (rf *Raft) SendRequestVoteToOther(i int, countChan chan bool, args * RequestVoteArgs, reply * RequestVoteReply) {
	if rf.sendRequestVote(i, args, reply) {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
		}
		if countChan != nil {
			countChan <- reply.VoteGranted
		}
	} else {
		if countChan != nil {
			countChan <- false
		}
	}
}
func (rf *Raft) Elect() {
	for ;true; {
		<-rf.timer.C // wait election timeout
		fmt.Printf("#%d server election timeout!\n", rf.me)
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
		rf.state = 1
		rf.currentTerm ++
		rf.votedFor = rf.me
		d := time.Duration(2000 * rand.Float64() + 1000)
		rf.timer = time.NewTimer(d * time.Millisecond)
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastIndex, LastLogTerm: rf.log[rf.lastIndex].Term}
		reply := &RequestVoteReply{}
		total := len(rf.peers)
		curTotal := 0
		count := 0
		countChan := make(chan bool, 10)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.SendRequestVoteToOther(i, countChan, args, reply)
		}

		for i := true; true ;i = <- countChan  {
			curTotal ++
			if i {
				count ++
			}
			if count > total / 2 || curTotal == total {
				break
			}
		}

		if count > total / 2 {
			rf.state = 2
			fmt.Printf("Leader chosen to be #%d server!\n", rf.me)
			go rf.Heartbeat()
		}
	}
	return
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Printf("make #%d server as a peer\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIndex = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = 0 // initiated as Follower
	rf.log = make([]Log, 1)
	rf.log[0].Term = 0
	var d time.Duration
	d = time.Duration(2000 * rand.Float64() + 1000)
	fmt.Printf("#%d server election timeout time %d\n", me, d)
	rf.timer = time.NewTimer(d * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf("start election for #%d server as a peer\n", me)
	go rf.Elect()

	return rf
}
