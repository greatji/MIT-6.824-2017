package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation	string
	Key		string
	Value		string
	OpId		int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStorage map[string]string
	getOpIdStorage map[int64]GetReply
	putAppendOpIdStorage map[int64]PutAppendReply
	result map[int]chan Op
}

func (kv *RaftKV) UpdateStorage() {
	for true {
		a := <- kv.applyCh
		kv.mu.Lock()
		//DPrintf("lock")
		log := a.Command.(Op)
		DPrintf("commited at server %d: %s, %v, %s, %s", kv.me, log.Value, log.OpId, log.Key, log.Operation)
		if _, yes := kv.putAppendOpIdStorage[log.OpId]; log.Operation == "Append" && !yes {
			v, ok := kv.kvStorage[log.Key]
			if ok {
				kv.kvStorage[log.Key] = v + log.Value
			} else {
				kv.kvStorage[log.Key] = log.Value
			}
			reply := PutAppendReply{WrongLeader: false, Err: OK}
			kv.putAppendOpIdStorage[log.OpId] = reply
		} else if _, yes := kv.putAppendOpIdStorage[log.OpId]; log.Operation == "Put" && !yes {
			kv.kvStorage[log.Key] = log.Value
			reply := PutAppendReply{WrongLeader: false, Err: OK}
			kv.putAppendOpIdStorage[log.OpId] = reply
		} else {
			value, ok := kv.kvStorage[log.Key]
			var reply GetReply
			if ok {
				reply = GetReply{WrongLeader: false, Value: value, Err: OK}
			} else {
				reply = GetReply{WrongLeader: false, Value: "", Err: ErrNoKey}
			}
			kv.getOpIdStorage[log.OpId] = reply
		}
		ch, ok := kv.result[a.Index]
		DPrintf("index: %d, %v", a.Index, ok)
		if ok {
			// clear the chan for the index
			select {
			case <-ch:
			default:
			}
			ch <- log
		} else {
			// when the kv is a follower
			kv.result[a.Index] = make(chan Op, 1)
		}
		//DPrintf("unlock")
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Get Operation Received at server %d: %s, %v", kv.me, args.Key, args.OpId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("%d is not the leader!", kv.me)
		reply.WrongLeader = true
		reply.Value = ""
		return
	} else {
		kv.mu.Lock()
		lastReply, ok := kv.getOpIdStorage[args.OpId]
		kv.mu.Unlock()
		if ok {
			reply.Value = lastReply.Value
			reply.Err = lastReply.Err
			reply.WrongLeader = lastReply.WrongLeader
			return
		}
		command := Op{Operation: "Get", Key: args.Key, Value: "", OpId: args.OpId}
		index, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			reply.WrongLeader = true
			reply.Value = ""
			return
		}

		kv.mu.Lock()
		ch, ok := kv.result[index]
		DPrintf("server %d putappend %v check index %d, found? %v", kv.me, args.OpId, index, ok)
		if !ok {
			ch = make(chan Op, 1)
			kv.result[index] = ch
		}
		kv.mu.Unlock()

		select {
		case op := <- ch:
			if op.OpId == args.OpId {
				kv.mu.Lock()
				lastReply, ok = kv.getOpIdStorage[args.OpId]
				kv.mu.Unlock()
				reply.WrongLeader = lastReply.WrongLeader
				reply.Value = lastReply.Value
				reply.Err = lastReply.Err
				return
			} else {
				reply.WrongLeader = true
				return
			}
		case <- time.After(2000 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%s Operation Received at server %d: %v, %v, %s", args.Op, kv.me, args.OpId, args.Key, args.Value)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("%d is not the leader!", kv.me)
		reply.WrongLeader = true
		return
	} else {
		kv.mu.Lock()
		lastReply, ok := kv.putAppendOpIdStorage[args.OpId]
		kv.mu.Unlock()
		if ok {
			reply.Err = lastReply.Err
			reply.WrongLeader = lastReply.WrongLeader
			return
		}
		reply.WrongLeader = false
		command := Op{Operation: args.Op, Key: args.Key, Value: args.Value, OpId: args.OpId}
		index, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			DPrintf("%d is not the leader while Start!", kv.me)
			reply.WrongLeader = true
			return
		}
		kv.mu.Lock()
		ch, ok := kv.result[index]
		DPrintf("server %d putappend %v check index %d, found? %v", kv.me, args.OpId, index, ok)
		if !ok {
			ch = make(chan Op, 1)
			kv.result[index] = ch
		}
		kv.mu.Unlock()

		select {
		case op := <- ch:
			if op.OpId == args.OpId {
				kv.mu.Lock()
				lastReply, ok = kv.putAppendOpIdStorage[args.OpId]
				kv.mu.Unlock()
				reply.WrongLeader = lastReply.WrongLeader
				reply.Err = lastReply.Err
				return
			} else {
				reply.WrongLeader = true
				return
			}
		case <- time.After(2000 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	kv.getOpIdStorage = make(map[int64]GetReply)
	kv.putAppendOpIdStorage = make(map[int64]PutAppendReply)
	kv.result = make(map[int]chan Op)
	go kv.UpdateStorage()
	return kv
}
