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
	//"fmt"
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
	offset		int

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

	applyChan	chan ApplyMsg


}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	//DPrintf("server %d GetState lock", rf.me)
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == 2
	//DPrintf("server %d GetState unlock", rf.me)
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
	e.Encode(rf.offset)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("persisted: server %d, currentTerm %d, votedFor %d, logLen %d, lastIndex, %d, offset: %d, logsize: %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.lastIndex, rf.offset, len(rf.persister.raftstate))
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
	d.Decode(&rf.offset)
}

//
// For Snapshot
//
func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) TakeSnapshot(data []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	DPrintf("Server #%d begin taking snapshot, loglen: %d, lastIndex: %d, offset: %d, lastIncludedIndex: %d, commitIndex: %d", rf.me, len(rf.log), rf.lastIndex, rf.offset, lastIncludedIndex, rf.commitIndex)
	defer rf.mu.Unlock()

	if lastIncludedIndex < rf.offset || lastIncludedIndex > rf.lastIndex {
		DPrintf("Server #%d complete taking snapshot, loglen: %d, lastIndex: %d, offset: %d, lastIncludedIndex: %d, commitIndex: %d", rf.me, len(rf.log), rf.lastIndex, rf.offset, lastIncludedIndex, rf.commitIndex)
		return
	}

	rf.log = rf.log[lastIncludedIndex - rf.offset : ] // discard old ones

	rf.offset = lastIncludedIndex

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	encoder.Encode(lastIncludedIndex)
	encoder.Encode(rf.log[0].Term)

	data = append(buf.Bytes(), data...)
	rf.persister.SaveSnapshot(data)

	rf.persist()

	DPrintf("Server #%d complete taking snapshot, loglen: %d, lastIndex: %d, offset: %d, lastIncludedIndex: %d, commitIndex: %d", rf.me, len(rf.log), rf.lastIndex, rf.offset, lastIncludedIndex, rf.commitIndex)

}

func (rf *Raft) ReadSnapshot(data []byte) {
	DPrintf("Server #%d begin reading snapshot, currentTerm: %d, lastIndex: %d, offset: %d, commitIndex: %d, logLen: %d", rf.me, rf.currentTerm, rf.lastIndex, rf.offset, rf.commitIndex, len(rf.log))

	if len(data) == 0 {
		DPrintf("Server #%d complete reading snapshot, no snapshot", rf.me)
		return
	}

	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)

	var lastIncludedIndex int
	var lastIncludedTerm int
	decoder.Decode(&lastIncludedIndex)
	decoder.Decode(&lastIncludedTerm)

	rf.mu.Lock()

	// rf.readPersist(rf.persister.ReadRaftState())

	if rf.lastApplied < lastIncludedIndex {
		rf.lastApplied = lastIncludedIndex
	}
	if rf.commitIndex < lastIncludedIndex {
		rf.commitIndex = lastIncludedIndex
	}

	rf.TruncateLog(lastIncludedIndex, lastIncludedTerm)

	rf.mu.Unlock()
	DPrintf("Server #%d complete reading snapshot, currentTerm: %d, lastIndex: %d, offset: %d, commitIndex: %d, logLen: %d", rf.me, rf.currentTerm, rf.lastIndex, rf.offset, rf.commitIndex, len(rf.log))
	DPrintf("Server #%d add usesnapshot msg to apply chan 205", rf.me)
	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	rf.applyChan <- msg
}

func (rf *Raft) TruncateLog(lastIncludedIndex int, lastIncludedTerm int) {

	////pivot := -1
	//first := Log{Term: lastIncludedTerm}
	//
	//if rf.lastIndex <= lastIncludedIndex {
	//	rf.offset = lastIncludedIndex
	//	rf.log = []Log{first}
	//	rf.lastIndex = lastIncludedIndex
	//	return
	//}
	//
	////for i := len(rf.log) - 1; i >= 0; i-- {
	////	if rf.log[i].LogIndex == lastIncludedIndex && rf.log[i].LogTerm == lastIncludedTerm {
	////		pivot = i
	////		break
	////	}
	////}
	//
	//DPrintf("Server #%d : lastIncludedIndex: %d, offset: %d, logLen: %d", rf.me, lastIncludedIndex, rf.offset, len(rf.log))
	//if lastIncludedIndex <= rf.offset {
	//	DPrintf("lastIncludedIndex sent by leader lower than myself, reject")
	//	return
	//}
	//if rf.log[lastIncludedIndex - rf.offset].Term != lastIncludedTerm {
	//	rf.log = []Log{first}
	//	rf.lastIndex = lastIncludedIndex
	//	rf.offset = lastIncludedIndex
	//} else {
	//	rf.log = append([]Log{first}, rf.log[lastIncludedIndex + 1 - rf.offset : ]...)
	//	rf.offset = lastIncludedIndex
	//}
	//
	//DPrintf("Server #%d : truncate log completed", rf.me)
	DPrintf("begin truncate, lastIncludedIndex %d, logLenth %d, lastIncludedTerm %d, lastIndex %d, offset %d, commitIndex: %d", lastIncludedIndex, len(rf.log), lastIncludedTerm, rf.lastIndex, rf.offset, rf.commitIndex)
	if lastIncludedIndex < rf.offset {
		DPrintf("Warning: Snapshot is lag behind, lastIncludedIndex %d, logLenth %d, lastIncludedTerm %d, lastIndex %d, offset %d, commitIndex: %d", lastIncludedIndex, len(rf.log), lastIncludedTerm, rf.lastIndex, rf.offset, rf.commitIndex)
		return
	}
	pivot := -1
	first := Log{Term: lastIncludedTerm}
	for i := len(rf.log) - 1; i >= 0; i-- {
		if i + rf.offset == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			pivot = i
			break
		}
	}

	if pivot < 0 {
		rf.log = []Log{first}
	} else {
		rf.log = append([]Log{first}, rf.log[pivot + 1 : ]...)
	}

	rf.offset = lastIncludedIndex
	rf.lastIndex = rf.offset + len(rf.log) - 1

	DPrintf("complete truncate, lastIncludedIndex %d, logLenth %d, lastIncludedTerm %d, lastIndex %d, offset %d, commitIndex: %d", lastIncludedIndex, len(rf.log), lastIncludedTerm, rf.lastIndex, rf.offset, rf.commitIndex)

}

type InstallSnapshotArgs struct {
	Term			int
	LeaderId		int
	LastIncludedIndex	int
	LastIncludedTerm	int
	ChunkOffset		int
	Data 			[]byte
	Done 			bool
}

type InstallSnapshotReply struct {
	Term 			int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server #%d got a Snapshot from Leader #%d", rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	if args.LastIncludedIndex <= rf.commitIndex {
		DPrintf("Warning: Snapshot is lag behind, lastIncludedIndex %d, logLenth %d, lastIncludedTerm %d, lastIndex %d, offset %d, commitIndex: %d", args.LastIncludedIndex, len(rf.log), args.LastIncludedTerm, rf.lastIndex, rf.offset, rf.commitIndex)
		return
	}
	if args.Term < rf.currentTerm {
		return
	}

	if args.LeaderId != rf.me {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
		rf.persist()
	} else {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
		rf.persist()
	}

	rf.persister.SaveSnapshot(args.Data)

	rf.TruncateLog(args.LastIncludedIndex, args.LastIncludedTerm)

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	DPrintf("Server #%d add usesnapshot msg to apply chan 293", rf.me)
	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.applyChan <- msg
	rf.persist()

	DPrintf("Server #%d complete a Snapshot from Leader #%d", rf.me, args.LeaderId)
}


func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("Leader #%d send install snapshot to server #%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != 2 || rf.currentTerm != args.Term {
		return ok
	}

	if rf.currentTerm < reply.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		rf.persist()
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
		return ok
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

	return ok
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
	//DPrintf("server %d RequestVote lock", rf.me)
	defer rf.mu.Unlock()
	DPrintf("#%d server receive RequestVote rpc from #%d at term %d\n", rf.me, args.CandidateId, args.Term)
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
		rf.persist()
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[rf.lastIndex - rf.offset].Term {
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.log[rf.lastIndex - rf.offset].Term && args.LastLogIndex >= rf.lastIndex {
			reply.VoteGranted = true
		} else {
			DPrintf("#%d server is more update than requestvoter\n", rf.me)
			reply.VoteGranted = false
		}
	} else {
		DPrintf("#%d server has voted other\n", rf.me)
		reply.VoteGranted = false
	}
	if (reply.VoteGranted) {
		rf.votedFor = args.CandidateId
		rf.state = 1
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
		rf.persist()
		var d time.Duration
		d = time.Duration(333 * rand.Float64() + 533)
		if rf.timer != nil {
			rf.timer.Reset(d * time.Millisecond)
		}
	}
	DPrintf("#%d server receive RequestVote rpc from #%d at term %d, result: %v\n", rf.me, args.CandidateId, rf.currentTerm, reply.VoteGranted)
	//DPrintf("server %d RequestVote unlock", rf.me)
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
	DPrintf("#%d server send requestvote rpc to #%d at term %d\n", args.CandidateId, server, args.Term)
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

	ConflictTerm	int // term of the conflicting entry
	ConflictIndex	int // first index it stores for that form
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("#%d server send appendentries rpc to #%d at term %d, isHeartBeat: %v\n", args.LeaderId, server, args.Term, len(args.Entries) == 0)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("#%d server receive appendentries rpc before lock from #%d at term %d, commitIndex: %d, preLogIndex; %d, currentLastIndex: %d %d, entryLen: %d, applyIndex: %d, isHeartbeat: %v\n", rf.me, args.LeaderId, args.Term, rf.commitIndex, args.PrevLogIndex, rf.lastIndex, len(rf.log), len(args.Entries), rf.lastApplied, len(args.Entries) == 0)
	rf.mu.Lock()
	//DPrintf("server %d AppendEntries lock", rf.me)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	DPrintf("#%d server receive appendentries rpc from #%d at term %d, currentTerm %d, commitIndex: %d, preLogIndex; %d, currentLastIndex: %d %d, entryLen: %d, applyIndex: %d, offset: %d, loglenth: %d, isHeartbeat: %v, offset: %d", rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.commitIndex, args.PrevLogIndex, rf.lastIndex, len(rf.log), len(args.Entries), rf.lastApplied, rf.offset, len(rf.log), len(args.Entries) == 0, rf.offset)
	// Your code here (2A, 2B).
	var d time.Duration
	d = time.Duration(333 * rand.Float64() + 533)
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
		rf.votedFor = -1
		rf.persist()
	} else if args.Term == rf.currentTerm && rf.state == 2 && args.LeaderId != rf.me {
		rf.currentTerm = args.Term
		rf.state = 0
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
		rf.votedFor = -1
		rf.persist()
	}
	if rf.timer != nil {
		rf.timer.Reset(d * time.Millisecond)
	}
	if rf.lastIndex < args.PrevLogIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIndex + 1
		reply.Success = false
		return
	} else if args.PrevLogIndex >= rf.offset && rf.log[args.PrevLogIndex - rf.offset].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex - rf.offset].Term
		reply.ConflictIndex = args.PrevLogIndex
		for reply.ConflictIndex - rf.offset > 1 {
			if rf.log[reply.ConflictIndex - rf.offset - 1].Term != reply.ConflictTerm {
				break
			}
			reply.ConflictIndex --
		}
		reply.Success = false
		return
	}

	entryLen := len(args.Entries)
	//if entryLen > 0 {
	entryPos := 0
	begin := args.PrevLogIndex + 1
	if args.PrevLogIndex < rf.offset {
		entryPos = rf.offset - args.PrevLogIndex
		begin = rf.offset + 1
	}
	for i := begin; i <= rf.lastIndex; i ++ {
		if entryPos >= entryLen {
			break
		}
		if rf.log[i - rf.offset].Term != args.Entries[entryPos].Term {
			DPrintf("#%d server mismatch at %v at index %d \n", rf.me, rf.log[i - rf.offset].Command, i)
			rf.log = rf.log[: i - rf.offset]
			rf.lastIndex = i - 1
			rf.persist()
			break
		}
		entryPos ++
	}
	DPrintf("#%d server mismatch after %v at index %d \n", rf.me, rf.log[rf.lastIndex - rf.offset].Command, rf.lastIndex)
	if entryPos < entryLen {
		DPrintf("#%d server replicated entries from leader, lastIndex becomes %d\n", rf.me, rf.lastIndex)
		for _, e := range args.Entries[entryPos :] {
			rf.log = append(rf.log, e)
			rf.lastIndex ++
			DPrintf("#%d server append %v at index %d \n", rf.me, e.Command, rf.lastIndex)
		}
	}
	//}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.lastIndex)))
	}
	reply.Success = true
	rf.persist()
	//DPrintf("server %d AppendEntries unlock", rf.me)
	return
}

func (rf *Raft) SendAppendEntryToAnother(i int, countCh chan bool, args * AppendEntriesArgs, reply * AppendEntriesReply) {
	if rf.state != 2 {
		return
	}
	ok := rf.sendAppendEntries(i, args, reply)
	rf.mu.Lock()
	//DPrintf("server %d SendAppendEntryToAnother lock", rf.me)
	defer rf.mu.Unlock()
	if rf.state != 2 {
		return
	}
	if /*rf.lastIndex >= rf.nextIndex[i] &&*/ ok {
		if rf.currentTerm < reply.Term {
			DPrintf("Leader #%d: Receive reply from server #%d, reply term is larger", rf.me, i)
			rf.state = 0
			rf.currentTerm = reply.Term
			if rf.leaderTimer != nil {
				rf.leaderTimer.Stop()
			}
			rf.votedFor = -1
			countCh <- false
		} else if reply.Success {
			DPrintf("Leader #%d: Receive reply from server #%d, success", rf.me, i)
			nextIndex := args.PrevLogIndex + len(args.Entries) + 1
			if nextIndex >= rf.nextIndex[i] {
				rf.nextIndex[i] = nextIndex
				rf.matchIndex[i] = nextIndex - 1
			}
			countCh <- true
		} else if args.Term >= reply.Term {
			DPrintf("Leader #%d: Receive reply from server #%d, not match retry it， conflictTerm %d, conflictIndex %d", rf.me, i, reply.ConflictTerm, reply.ConflictIndex)
			for k := args.PrevLogIndex - 1; k >= rf.offset + 1; k -- {
				if rf.log[k - rf.offset].Term == reply.ConflictTerm {
					args.PrevLogIndex = k
					break
				}
			}
			if args.PrevLogIndex >= rf.offset && rf.log[args.PrevLogIndex - rf.offset].Term != reply.ConflictTerm {
				args.PrevLogIndex = reply.ConflictIndex - 1
			}
			DPrintf("before out of range: lastIndex: %d prevLogIndex: %d offset: %d logLength: %d", rf.lastIndex, args.PrevLogIndex, rf.offset, len(rf.log))
			if args.PrevLogIndex < rf.offset {
				DPrintf("Server #%d is lag behind, wait for SendSnapshot", i)
				countCh <- false
				rf.nextIndex[i] = reply.ConflictIndex
				return
			}
			args.Entries = rf.log[args.PrevLogIndex + 1 - rf.offset : rf.lastIndex + 1 - rf.offset]
			args.PrevLogTerm = rf.log[args.PrevLogIndex - rf.offset].Term
			go rf.SendAppendEntryToAnother(i, countCh, args, reply)

		} else {
			DPrintf("Expired request, canceled")
			countCh <- false
		}
	} else /*if rf.lastIndex >= rf.nextIndex[i]*/ {
		DPrintf("Leader #%d: Receive reply from server #%d, network block but retry it", rf.me, i)
		go rf.SendAppendEntryToAnother(i, countCh, args, reply)
		//countCh <- false
	} /*else {
		DPrintf("Leader #%d: Receive reply from server #%d, no more entries needed success", rf.me, i)
		countCh <- true
	}*/
	//DPrintf("server %d SendAppendEntryToAnother unlock", rf.me)
}
func (rf *Raft) Agreement() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("server %d Agreement lock", rf.me)
	if rf.state != 2 {
		return
	}
	countCh := make(chan bool, 10)
	for i := range rf.peers {
		if rf.nextIndex[i] > rf.offset {
			DPrintf("Server %d send append entry to server %d, nextIndex: %d, offset: %d, lastIndex: %d, logLen: %d", rf.me, i, rf.nextIndex[i], rf.offset, rf.lastIndex, len(rf.log))
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[i] - 1, PrevLogTerm: rf.log[rf.nextIndex[i] - 1 - rf.offset].Term, LeaderCommit: rf.commitIndex, Entries: rf.log[rf.nextIndex[i] - rf.offset : rf.lastIndex + 1 - rf.offset]}
			reply := &AppendEntriesReply{}
			go rf.SendAppendEntryToAnother(i, countCh, args, reply)
		} else {
			DPrintf("Leader #%d, for server #%d, commited log do not match? Sending Snapshot", rf.me, i)
			snapshotArgs := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.offset, LastIncludedTerm: rf.log[0].Term, ChunkOffset: 0, Data: rf.persister.snapshot, Done: true}
			go func(server int, arg *InstallSnapshotArgs) {
				var reply InstallSnapshotReply
				rf.SendInstallSnapshot(server, arg, &reply)
			}(i, &snapshotArgs)
		}
	}
	go func() {
		positiveCount := 0
		count := 0
		total := len(rf.peers)
		for a := <-countCh; true; a = <-countCh {
			count ++
			if a {
				positiveCount ++
				DPrintf("Leader #%d Get Positive reply, %d replicated success, %d total reply", rf.me, positiveCount, count)
			} else {
				DPrintf("Leader #%d Get Negative reply, %d replicated success, %d total reply", rf.me, positiveCount, count)
			}
			if positiveCount > total / 2 || count == total {
				break
			}
		}
		rf.mu.Lock()
		if rf.state == 2 {
			for N := rf.lastIndex; N > rf.commitIndex; N -- {
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
				DPrintf("N: %d, commitIndex: %d, lastIndex: %d, offset: %d", N, rf.commitIndex, rf.lastIndex, rf.offset)
				if count > total / 2 && rf.log[N - rf.offset].Term == rf.currentTerm {
					rf.commitIndex = N
					break
				}
			}
			DPrintf("Leader #%d commit at index: %d", rf.me, rf.commitIndex)
		} else {
			DPrintf("Leader #%d has become %d", rf.me, rf.state)
		}
		rf.persist()
		rf.mu.Unlock()
	}()
	//DPrintf("server %d Agreement unlock", rf.me)
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
	//DPrintf("server %d Start lock", rf.me)
	defer rf.mu.Unlock()
	term := rf.currentTerm
	//isLeader := true
	// Your code here (2B).
	isLeader := rf.state == 2
	if isLeader {
		rf.log = append(rf.log, Log{Term: rf.currentTerm, Command: command})
		rf.lastIndex ++
		rf.nextIndex[rf.me] = rf.lastIndex + 1
		rf.matchIndex[rf.me] = rf.lastIndex
		DPrintf("#%d leader get a client request, and commitIndex: %d, lastIndex: %d, applyIndex: %d, offset: %d, insert: %d", rf.me, rf.commitIndex, rf.lastIndex, rf.lastApplied, rf.offset, rf.log[rf.lastIndex - rf.offset].Command)
		rf.persist()
		go rf.Agreement()
	}
	//DPrintf("server %d Start unlock", rf.me)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = 0
	if rf.timer != nil {
		rf.timer.Stop()
	}
	if rf.leaderTimer != nil {
		rf.leaderTimer.Stop()
	}
	rf.persist()
}

func (rf *Raft) SendHeartbeatToAnother(i int, args * AppendEntriesArgs, reply * AppendEntriesReply) {
	ok := rf.sendAppendEntries(i, args, reply)
	rf.mu.Lock()
	//DPrintf("server %d SendHeartbeatToAnother lock", rf.me)
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0
			if rf.leaderTimer != nil {
				rf.leaderTimer.Stop()
			}
			rf.votedFor = -1
		}
	}
	//DPrintf("server %d SendHeartbeatToAnother unlock", rf.me)
}

func (rf *Raft) Heartbeat() {
	for rf.state == 2 {
		//DPrintf("server %d Hearbeat lock", rf.me)
		rf.leaderTimer = time.NewTimer(50 * time.Millisecond)
		rf.Agreement()
		//for i := range rf.peers {
		//	args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.lastIndex, PrevLogTerm: rf.log[rf.lastIndex].Term, LeaderCommit: rf.commitIndex}
		//	reply := &AppendEntriesReply{}
		//	go rf.SendHeartbeatToAnother(i, args, reply)
		//}
		//DPrintf("server %d Hearbeat unlock", rf.me)
		<- rf.leaderTimer.C
	}
}

func (rf *Raft) SendRequestVoteToOther(i int, countChan chan bool, args * RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(i, args, reply)
	rf.mu.Lock()
	//DPrintf("server %d SendRequestVoteToOther lock", rf.me)
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.votedFor = -1
			if rf.leaderTimer != nil {
				rf.leaderTimer.Stop()
			}
			if countChan != nil {
				DPrintf("#%d server receive vote from #%d server at term %d, turn to Follower\n", rf.me, i, args.Term)
				countChan <- false
			}
		} else if args.Term < rf.currentTerm {
			if countChan != nil {
				DPrintf("#%d server receive false vote from #%d server at term %d because the reply is expired\n", rf.me, i, args.Term)
				countChan <- false
			}
		} else {
			if countChan != nil {
				DPrintf("#%d server receive %v vote from #%d server at term %d\n", rf.me, reply.VoteGranted, i, args.Term)
				countChan <- reply.VoteGranted
			}
		}
	} else {
		if countChan != nil {
			DPrintf("#%d server cannot receive vote from #%d server at term %d, network is block\n", rf.me, i, args.Term)
			countChan <- false
		}
	}
	//DPrintf("server %d SendRequestVoteToOther unlock", rf.me)
}

func (rf *Raft) ApplyEntry(applyCh chan ApplyMsg) {
	for true {
		rf.mu.Lock()
		//DPrintf("server %d ApplyEntry lock", rf.me)
		if rf.lastApplied < rf.offset {
			rf.lastApplied = rf.offset
		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied ++
			DPrintf("Server #%d add msg to apply chan 817, lastapplied: %d, lastIndex: %d, offset: %d, commitIndex: %d, logLen: %d", rf.me, rf.lastApplied, rf.lastIndex, rf.offset, rf.commitIndex, len(rf.log))
			applyCh <- ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied - rf.offset].Command}
		}
		//DPrintf("server %d ApplyEntry unlock", rf.me)
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) Elect() {
	rf.GetNewElectionTimer()
	for ;true; {
		<-rf.timer.C // wait election timeout
		rf.mu.Lock()
		//DPrintf("server %d Elect lock", rf.me)
		DPrintf("#%d server election timeout!\n", rf.me)
		rf.state = 1
		if rf.leaderTimer != nil {
			rf.leaderTimer.Stop()
		}
		rf.currentTerm ++
		rf.votedFor = rf.me
		rf.GetNewElectionTimer()
		DPrintf("#%d server, currentTerm: %d, candidateId: %d, lastLogIndex: %d, offset: %d, logLen: %d", rf.me, rf.currentTerm, rf.me, rf.lastIndex, rf.offset, len(rf.log))
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastIndex, LastLogTerm: rf.log[rf.lastIndex - rf.offset].Term}
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
				if count > total / 2 || curTotal == total || rf.state != 1{
					break
				}
			}

			if count > total / 2 && rf.state == 1 {
				rf.mu.Lock()
				//DPrintf("server %d Elect_inner lock", rf.me)
				rf.state = 2
				DPrintf("Leader chosen to be #%d server! count: %d, total: %d\n", rf.me, count, total)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i ++ {
					rf.nextIndex[i] = rf.lastIndex + 1
					rf.matchIndex[i] = 0
				}
				go rf.Heartbeat()
				//DPrintf("server %d Elect_inner unlock", rf.me)
				rf.mu.Unlock()
			}
		}()
		//DPrintf("server %d Elect unlock", rf.me)
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
	rf.timer = time.NewTimer(time.Duration(333 * rand.Float64() + 533) * time.Millisecond)
}


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//fmt.Printf("make #%d server as a peer\n", me)
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
	rf.offset = 0
	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ReadSnapshot(persister.ReadSnapshot())

	//fmt.Printf("start election for #%d server as a peer\n", me)
	go rf.Elect()
	go rf.ApplyEntry(applyCh)

	return rf
}
