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
	"bytes"
	"encoding/gob"
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastIndex)
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
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	fmt.Printf("#%d server receive RequestVote rpc from #%d at term %d\n", rf.me, args.CandidateId, args.Term)
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[rf.lastIndex].Term {
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.log[rf.lastIndex].Term && args.LastLogIndex >= rf.lastIndex {
			reply.VoteGranted = true
		} else {
			fmt.Printf("#%d server is more update than requestvoter\n", rf.me)
			reply.VoteGranted = false
		}
	} else {
		fmt.Printf("#%d server has voted other\n", rf.me)
		reply.VoteGranted = false
	}
	if (reply.VoteGranted) {
		rf.votedFor = args.CandidateId
		rf.state = 1
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
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
	fmt.Printf("#%d server send appendentries rpc to #%d at term %d, isHeartBeat: %v\n", args.LeaderId, server, args.Term, len(args.Entries) == 0)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	fmt.Printf("#%d server receive appendentries rpc from #%d at term %d, preLogIndex; %d, currentLastIndex: %d %d, entryLen: %d, isHeartbeat: %v\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, rf.lastIndex, len(rf.log), len(args.Entries), len(args.Entries) == 0)
	// Your code here (2A, 2B).
	var d time.Duration
	d = time.Duration(2000 * rand.Float64() + 1000)
	if args.Term < rf.currentTerm {
		// expired rpc
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
	}
	rf.timer.Reset(d * time.Millisecond)
	if !(rf.lastIndex >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	entryLen := len(args.Entries)
	if entryLen > 0 {
		entryPos := 0
		for i := args.PrevLogIndex + 1; i <= rf.lastIndex; i ++ {
			if entryPos >= entryLen {
				break
			}
			if rf.log[i].Term != args.Entries[entryPos].Term {
				fmt.Printf("#%d server mismatch at %v at index %d \n", rf.me, rf.log[i].Command, i)
				rf.log = rf.log[: i]
				rf.lastIndex = i - 1
				break
			}
			entryPos ++
		}
		fmt.Printf("#%d server mismatch after %v at index %d \n", rf.me, rf.log[rf.lastIndex].Command, rf.lastIndex)
		for _, e := range args.Entries[entryPos :] {
			rf.log = append(rf.log, e)
			rf.lastIndex ++
			fmt.Printf("#%d server append %v at index %d \n", rf.me, e.Command, rf.lastIndex)
		}
		fmt.Printf("#%d server replicated entries from leader, lastIndex becomes %d\n", rf.me, rf.lastIndex)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.lastIndex)))
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

func (rf *Raft) SendAppendEntryToAnother(i int, countCh chan bool, lastIndex int) {
	rf.mu.Lock()
	nextIndex := rf.nextIndex[i]
	log := make([]Log, len(rf.log))
	copy(log, rf.log)
	commitIndex := rf.commitIndex
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if lastIndex >= nextIndex {
		args := &AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me, PrevLogIndex: nextIndex - 1, PrevLogTerm: log[nextIndex - 1].Term, LeaderCommit: commitIndex, Entries: log[nextIndex:lastIndex + 1]}
		reply := &AppendEntriesReply{}
		for !rf.sendAppendEntries(i, args, reply) {}
		for ; !reply.Success && reply.Term <= currentTerm; {
			nextIndex --
			args = &AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me, PrevLogIndex: nextIndex - 1, PrevLogTerm: log[nextIndex - 1].Term, LeaderCommit: commitIndex, Entries: log[nextIndex:lastIndex + 1]}
			if !rf.sendAppendEntries(i, args, reply) {
				break
			}
		}
		if reply.Success {
			rf.mu.Lock()
			if rf.nextIndex[i] < lastIndex + 1 {
				rf.nextIndex[i] = lastIndex + 1
			}
			if rf.matchIndex[i] < lastIndex {
				rf.matchIndex[i] = lastIndex
			}
			rf.mu.Unlock()
			countCh <- true
		} else {
			countCh <- false
		}
	} else {
		countCh <- false
	}
}
func (rf *Raft) Agreement(command interface{}, lastIndex int) {
	fmt.Printf("#%d leader get a client request, and commitIndex: %d, lastIndex: %d, applyIndex: %d, insert: %d\n", rf.me, rf.commitIndex, lastIndex, rf.lastApplied, rf.log[lastIndex].Command)
	countCh := make(chan bool, 10)
	for i := range rf.peers {
		if i == rf.me {
			continue
		} else {
			go rf.SendAppendEntryToAnother(i, countCh, lastIndex)
		}
	}
	positiveCount := 0
	count := 0
	total := len(rf.peers)
	for a := true; true; a = <- countCh{
		if a {
			positiveCount ++
		}
		count ++
		if positiveCount > total / 2 || count == total {
			break
		}
	}
	rf.mu.Lock()
	for N := lastIndex; N > rf.commitIndex; N -- {
		count := 0
		total := len(rf.peers)
		for _, v := range rf.matchIndex {
			if v >= N {
				count ++
				if count > total / 2 {
					break
				}
			}
		}
		if count > total / 2 && rf.log[N].Term == rf.currentTerm {
			if rf.commitIndex < N {
				rf.commitIndex = N
			}
			break
		}
	}
	rf.mu.Unlock()
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
	//index := rf.lastIndex + 1
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	//isLeader := true
	// Your code here (2B).
	isLeader := rf.state == 2
	if isLeader {
		rf.log = append(rf.log, Log{Term: rf.currentTerm, Command: command})
		rf.lastIndex ++
		li := rf.lastIndex
		rf.nextIndex[rf.me] = rf.lastIndex + 1
		rf.matchIndex[rf.me] = rf.lastIndex
		go rf.Agreement(command, li)
	}
	return rf.lastIndex, term, isLeader
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

func (rf *Raft) SendHeartbeatToAnother(i int, args * AppendEntriesArgs, reply * AppendEntriesReply) {
	if (rf.sendAppendEntries(i, args, reply)) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0
			if rf.leaderTimer != nil {
				rf.leaderTimer.Stop()
			}
		}
	}
}
func (rf *Raft) Heartbeat() {
	for _, v := rf.GetState(); v; {
		rf.leaderTimer = time.NewTimer(100 * time.Millisecond)
		rf.mu.Lock()
		args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.lastIndex, PrevLogTerm: rf.log[rf.lastIndex].Term, LeaderCommit: rf.commitIndex}
		reply := &AppendEntriesReply{}
		for i := range rf.peers {
			go rf.SendHeartbeatToAnother(i, args, reply)
		}
		rf.mu.Unlock()
		<- rf.leaderTimer.C
	}
}

func (rf *Raft) SendRequestVoteToOther(i int, countChan chan bool, args * RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(i, args, reply) {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0
		}
		if countChan != nil {
			fmt.Printf("%d server vote as %v\n", i, reply.VoteGranted)
			countChan <- reply.VoteGranted
		}
	} else {
		if countChan != nil {
			fmt.Printf("%d server vote as false\n", i)
			countChan <- false
		}
	}
}

func (rf *Raft) ApplyEntry(applyCh chan ApplyMsg) {
	t := time.NewTimer(time.Duration(10) * time.Millisecond)
	for true {
		<- t.C
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied ++
			applyCh <- ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied].Command}
		}
		rf.mu.Unlock()
		t = time.NewTimer(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) Elect() {
	rf.GetNewElectionTimer()
	for ;true; {
		<-rf.timer.C // wait election timeout
		rf.mu.Lock()
		fmt.Printf("#%d server election timeout!\n", rf.me)
		rf.state = 1
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
		rf.currentTerm ++
		rf.votedFor = rf.me
		rf.GetNewElectionTimer()
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastIndex, LastLogTerm: rf.log[rf.lastIndex].Term}
		total := len(rf.peers)
		countChan := make(chan bool, 10)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.SendRequestVoteToOther(i, countChan, args)
		}
		go func() {
			curTotal := 0
			count := 0
			for i := true; true; i = <-countChan {
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
				fmt.Printf("Leader chosen to be #%d server! count: %d, total: %d\n", rf.me, count, total)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i ++ {
					rf.nextIndex[i] = rf.lastIndex + 1
					rf.matchIndex[i] = 0
				}
				go rf.Heartbeat()
			}
		}()
		rf.mu.Unlock()
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

func (rf * Raft)GetNewElectionTimer() {
	rf.timer = time.NewTimer(time.Duration(2000 * rand.Float64() + 1000) * time.Millisecond)
}


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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf("start election for #%d server as a peer\n", me)
	go rf.Elect()
	go rf.ApplyEntry(applyCh)

	return rf
}
