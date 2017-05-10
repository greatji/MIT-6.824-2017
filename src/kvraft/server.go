package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 0

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
	OperationId	int
	ClientId	int64
}

type Result struct {
	OperationId	int
	ClientId	int64
	Value 		string
	Error 		Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStorage map[string]string
	//getOpIdStorage map[int64]GetReply
	//putAppendOpIdStorage map[int64]PutAppendReply
	historyRecord	map[int64]int
	result map[int]chan Result
}

func (kv *RaftKV) CheckDuplicated(clientId int64, operationId int) bool {
	value, ok := kv.historyRecord[clientId]
	if !ok {
		return false
	}
	if value < operationId {
		return false
	} else {
		return true
	}
}

func (kv *RaftKV) UpdateStorage() {
	for true {
		a := <- kv.applyCh
		DPrintf("Kvserver #%d, Get message", kv.me)
		if a.UseSnapshot {

			var lastIncludedIndex int
			var lastIncludedTerm int
			snapshot := bytes.NewBuffer(a.Snapshot)
			decoder := gob.NewDecoder(snapshot)
			decoder.Decode(&lastIncludedIndex)
			decoder.Decode(&lastIncludedTerm)

			kv.mu.Lock()
			kv.kvStorage = make(map[string]string)
			//kv.getOpIdStorage = make(map[int64]GetReply)
			//kv.putAppendOpIdStorage = make(map[int64]PutAppendReply)
			kv.historyRecord = make(map[int64]int)

			decoder.Decode(&kv.kvStorage)
			//decoder.Decode(&kv.getOpIdStorage)
			//decoder.Decode(&kv.putAppendOpIdStorage)
			decoder.Decode(&kv.historyRecord)
			kv.mu.Unlock()

			continue
		}

		kv.mu.Lock()
		//DPrintf("lock")
		log := a.Command.(Op)
		DPrintf("commited at server %d: %s, %v, %d, %s, %s", kv.me, log.Value, log.ClientId, log.OperationId, log.Key, log.Operation)
		var result Result
		result.OperationId = log.OperationId
		result.ClientId = log.ClientId
		if !kv.CheckDuplicated(log.ClientId, log.OperationId) && log.Operation == "Append" {
			v, ok := kv.kvStorage[log.Key]
			if ok {
				kv.kvStorage[log.Key] = v + log.Value
			} else {
				kv.kvStorage[log.Key] = log.Value
			}
		} else if !kv.CheckDuplicated(log.ClientId, log.OperationId) && log.Operation == "Put" {
			kv.kvStorage[log.Key] = log.Value
		} else {
			value, ok := kv.kvStorage[log.Key]
			if ok {
				result.Error = OK
				result.Value = value
			} else {
				result.Value = ""
				result.Error = ErrNoKey
			}
		}
		if !kv.CheckDuplicated(log.ClientId, log.OperationId) {
			kv.historyRecord[log.ClientId] = log.OperationId
		}
		ch, ok := kv.result[a.Index]
		DPrintf("index: %d, %v", a.Index, ok)
		if ok {
			// clear the chan for the index
			select {
			case <-ch:
			default:
			}
			ch <- result
		} else {
			// when the kv is a follower
			kv.result[a.Index] = make(chan Result, 1)
		}

		if kv.maxraftstate != -1 && kv.maxraftstate < kv.rf.GetPersistSize() {
			buf := new(bytes.Buffer)
			encoder := gob.NewEncoder(buf)
			encoder.Encode(kv.kvStorage)
			//encoder.Encode(kv.getOpIdStorage)
			//encoder.Encode(kv.putAppendOpIdStorage)
			encoder.Encode(kv.historyRecord)
			go kv.rf.TakeSnapshot(buf.Bytes(), a.Index)
		}
		//DPrintf("unlock")
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Get Operation Received at server %d: %s, %v, %d", kv.me, args.Key, args.ClientId, args.OperationId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("%d is not the leader!", kv.me)
		reply.WrongLeader = true
		reply.Value = ""
		return
	} else {
		//kv.mu.Lock()
		//lastReply, ok := kv.getOpIdStorage[args.OpId]
		//kv.mu.Unlock()
		//if ok {
		//	reply.Value = lastReply.Value
		//	reply.Err = lastReply.Err
		//	reply.WrongLeader = lastReply.WrongLeader
		//	return
		//}
		command := Op{Operation: "Get", Key: args.Key, Value: "", OperationId: args.OperationId, ClientId: args.ClientId}
		index, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			reply.WrongLeader = true
			reply.Value = ""
			return
		}
		kv.mu.Lock()
		ch, ok := kv.result[index]
		DPrintf("server %d putappend %v %d check index %d, found? %v", kv.me, args.ClientId, args.OperationId, index, ok)
		if !ok {
			ch = make(chan Result, 1)
			kv.result[index] = ch
		}
		kv.mu.Unlock()

		select {
		case result := <- ch:
			if result.ClientId == args.ClientId && result.OperationId == args.OperationId {
				reply.WrongLeader = false
				reply.Value = result.Value
				reply.Err = result.Error
				return
			} else {
				reply.WrongLeader = true
				return
			}
		case <- time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%s Operation Received at server %d: %v, %d, %v, %s", args.Op, kv.me, args.ClientId, args.OperationId, args.Key, args.Value)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("%d is not the leader!", kv.me)
		reply.WrongLeader = true
		return
	} else {
		command := Op{Operation: args.Op, Key: args.Key, Value: args.Value, OperationId: args.OperationId, ClientId: args.ClientId}
		index, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			DPrintf("%d is not the leader while Start!", kv.me)
			reply.WrongLeader = true
			return
		}
		kv.mu.Lock()
		ch, ok := kv.result[index]
		DPrintf("server %d putappend %v %d check index %d, found? %v", kv.me, args.ClientId, args.OperationId, index, ok)
		if !ok {
			ch = make(chan Result, 1)
			kv.result[index] = ch
		}
		kv.mu.Unlock()

		select {
		case result := <- ch:
			if result.ClientId == args.ClientId && result.OperationId == args.OperationId {
				reply.WrongLeader = false
				reply.Err = result.Error
				return
			} else {
				reply.WrongLeader = true
				return
			}
		case <- time.After(1000 * time.Millisecond):
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

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	//kv.getOpIdStorage = make(map[int64]GetReply)
	//kv.putAppendOpIdStorage = make(map[int64]PutAppendReply)
	kv.historyRecord = make(map[int64]int)
	kv.result = make(map[int]chan Result)
	go kv.UpdateStorage()
	return kv
}
