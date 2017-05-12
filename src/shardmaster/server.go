package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"bytes"
	"log"
	"time"
	"math"
)


const (
	QUERY = "QUERY"
	JOIN = "JOIN"
	LEAVE = "LEAVE"
	MOVE = "MOVE"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int

	// Your data here.
	historyRecord	map[int64]int
	result map[int]chan Result

	configs []Config // indexed by config num

}
type Result struct {
	OperationId	int
	ClientId	int64
	// for query operation
	Config      	Config
	Error 		Err
}

type Op struct {
	// Your data here.
	Operation	string
	// for join operation
	Servers 	map[int][]string
	// for leave operation
	GIDs 		[]int
	// for move operation
	Shard 		int
	GID   		int
	// for query operation
	Num int

	OperationId	int
	ClientId	int64
}


func (sm *ShardMaster) CheckDuplicated(clientId int64, operationId int) bool {
	value, ok := sm.historyRecord[clientId]
	if !ok {
		return false
	}
	if value < operationId {
		return false
	} else {
		return true
	}
}

func (sm *ShardMaster) getGidMinShards(shardsNumEachGroup map[int]int) (gid int, value int) {
	gid = math.MinInt64
	value = math.MaxInt64
	for k, v := range shardsNumEachGroup {
		if v < value {
			gid = k
			value = v
		} else if v == value && k < gid{
			gid = k
			value = v
		}
	}
	return
}

func (sm *ShardMaster) UpdateStorage() {
	for true {
		a := <- sm.applyCh
		DPrintf("Smserver #%d, Get message", sm.me)
		if a.UseSnapshot {

			var lastIncludedIndex int
			var lastIncludedTerm int
			snapshot := bytes.NewBuffer(a.Snapshot)
			decoder := gob.NewDecoder(snapshot)
			decoder.Decode(&lastIncludedIndex)
			decoder.Decode(&lastIncludedTerm)

			sm.mu.Lock()
			sm.configs = make([]Config, 0)
			//kv.getOpIdStorage = make(map[int64]GetReply)
			//kv.putAppendOpIdStorage = make(map[int64]PutAppendReply)
			sm.historyRecord = make(map[int64]int)

			decoder.Decode(&sm.configs)
			//decoder.Decode(&kv.getOpIdStorage)
			//decoder.Decode(&kv.putAppendOpIdStorage)
			decoder.Decode(&sm.historyRecord)
			sm.mu.Unlock()

			continue
		}

		sm.mu.Lock()
		//DPrintf("lock")
		log := a.Command.(Op)
		DPrintf("commited at server %d: %v, %d, %s", sm.me, log.ClientId, log.OperationId, log.Operation)
		var result Result
		result.OperationId = log.OperationId
		result.ClientId = log.ClientId
		if !sm.CheckDuplicated(log.ClientId, log.OperationId) && log.Operation == JOIN {
			DPrintf("joining at server %d: %v, %d, %s, addServersNum: %d", sm.me, log.ClientId, log.OperationId, log.Operation, len(log.Servers))
			newConfig := Config{}
			newConfig.Groups = map[int][]string{}
			for k, v := range sm.configs[len(sm.configs) - 1].Groups {
				newConfig.Groups[k] = v
			}
			for k, v := range log.Servers {
				newConfig.Groups[k] = v
			}
			newConfig.Num = sm.configs[len(sm.configs) - 1].Num + 1
			newConfig.Shards = sm.configs[len(sm.configs) - 1].Shards
			// evenly redistribute the shards code
			groupNum := len(newConfig.Groups)
			eachNum := NShards / groupNum
			shardsNumEachGroup := make(map[int]int)
			for i, v := range newConfig.Shards {
				if v != 0 {
					value, ok := shardsNumEachGroup[v]
					if ok {
						if value < eachNum {
							shardsNumEachGroup[v] = value + 1
						} else {
							newConfig.Shards[i] = 0
						}
					} else {
						shardsNumEachGroup[v] = 1
					}
				}
			}
			DPrintf("147 %v, groupNum: %d, NShards: %d", newConfig.Shards, groupNum, NShards)
			for k := range newConfig.Groups {
				_, ok := shardsNumEachGroup[k]
				if !ok {
					shardsNumEachGroup[k] = 0
				}
			}
			for i, v := range newConfig.Shards {
				if v == 0 {
					gid, value := sm.getGidMinShards(shardsNumEachGroup)
					shardsNumEachGroup[gid] = value + 1
					newConfig.Shards[i] = gid
				}
			}
			sm.configs = append(sm.configs, newConfig)
		} else if !sm.CheckDuplicated(log.ClientId, log.OperationId) && log.Operation == LEAVE {
			DPrintf("leaving at server %d: %v, %d, %s, %v", sm.me, log.ClientId, log.OperationId, log.Operation, log.GIDs)
			newConfig := Config{}
			newConfig.Groups = map[int][]string{}
			deleteMap := make(map[int]bool)
			for _, v := range log.GIDs {
				deleteMap[v] = true
			}
			for k, v := range sm.configs[len(sm.configs) - 1].Groups {
				_, ok := deleteMap[k]
				if !ok {
					newConfig.Groups[k] = v
				}
			}
			newConfig.Num = sm.configs[len(sm.configs) - 1].Num + 1
			newConfig.Shards = sm.configs[len(sm.configs) - 1].Shards
			// evenly redistribute the shards code
			shardsNumEachGroup := make(map[int]int)
			for k := range newConfig.Groups {
				_, ok := shardsNumEachGroup[k]
				if !ok {
					shardsNumEachGroup[k] = 0
				}
			}
			for i, v := range newConfig.Shards {
				value, ok := shardsNumEachGroup[v]
				if ok {
					shardsNumEachGroup[v] = value + 1
				} else {
					newConfig.Shards[i] = 0
				}
			}
			for i, v := range newConfig.Shards {
				if v == 0 {
					gid, value := sm.getGidMinShards(shardsNumEachGroup)
					shardsNumEachGroup[gid] = value + 1
					newConfig.Shards[i] = gid
				}
			}
			sm.configs = append(sm.configs, newConfig)
		} else if !sm.CheckDuplicated(log.ClientId, log.OperationId) && log.Operation == MOVE {
			DPrintf("moving at server %d: %v, %d, %s", sm.me, log.ClientId, log.OperationId, log.Operation)
			newConfig := Config{}
			newConfig.Groups = map[int][]string{}
			for k, v := range sm.configs[len(sm.configs) - 1].Groups {
				newConfig.Groups[k] = v
			}
			newConfig.Shards = sm.configs[len(sm.configs) - 1].Shards
			newConfig.Shards[log.Shard] = log.GID
			newConfig.Num = sm.configs[len(sm.configs) - 1].Num + 1
			// evenly redistribute the shards code
			sm.configs = append(sm.configs, newConfig)
		} else if log.Operation == QUERY {
			DPrintf("querying at server %d: %v, %d, %s, num: %d, configLen: %d", sm.me, log.ClientId, log.OperationId, log.Operation, log.Num, len(sm.configs))
			if log.Num == -1 || log.Num >= len(sm.configs) {
				result.Config = sm.configs[len(sm.configs) - 1]
			} else {
				result.Config = sm.configs[log.Num]
			}
		}
		if !sm.CheckDuplicated(log.ClientId, log.OperationId) {
			sm.historyRecord[log.ClientId] = log.OperationId
		}
		ch, ok := sm.result[a.Index]
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
			sm.result[a.Index] = make(chan Result, 1)
		}

		if sm.maxraftstate != -1 && sm.maxraftstate < sm.rf.GetPersistSize() {
			buf := new(bytes.Buffer)
			encoder := gob.NewEncoder(buf)
			encoder.Encode(sm.configs)
			//encoder.Encode(kv.getOpIdStorage)
			//encoder.Encode(kv.putAppendOpIdStorage)
			encoder.Encode(sm.historyRecord)
			go sm.rf.TakeSnapshot(buf.Bytes(), a.Index)
		}
		//DPrintf("unlock")
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("JOIN Operation Received at server %d: %v, %d", sm.me, args.ClientId, args.OperationId)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		DPrintf("%d is not the leader!", sm.me)
		reply.WrongLeader = true
		return
	} else {
		command := Op{}
		command.Servers = make(map[int][]string)
		for k, v := range args.Servers {
			command.Servers[k] = v
		}
		command.OperationId = args.OperationId
		command.ClientId = args.ClientId
		command.Operation = JOIN

		index, _, isLeader := sm.rf.Start(command)
		if !isLeader {
			DPrintf("%d is not the leader while Start!", sm.me)
			reply.WrongLeader = true
			return
		}
		sm.mu.Lock()
		ch, ok := sm.result[index]
		DPrintf("server %d join %v %d check index %d, found? %v", sm.me, args.ClientId, args.OperationId, index, ok)
		if !ok {
			ch = make(chan Result, 1)
			sm.result[index] = ch
		}
		sm.mu.Unlock()

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

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("LEAVE Operation Received at server %d: %v, %d", sm.me, args.ClientId, args.OperationId)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		DPrintf("%d is not the leader!", sm.me)
		reply.WrongLeader = true
		return
	} else {
		command := Op{}
		command.GIDs = args.GIDs
		command.OperationId = args.OperationId
		command.ClientId = args.ClientId
		command.Operation = LEAVE

		index, _, isLeader := sm.rf.Start(command)
		if !isLeader {
			DPrintf("%d is not the leader while Start!", sm.me)
			reply.WrongLeader = true
			return
		}
		sm.mu.Lock()
		ch, ok := sm.result[index]
		DPrintf("server %d leave %v %d check index %d, found? %v", sm.me, args.ClientId, args.OperationId, index, ok)
		if !ok {
			ch = make(chan Result, 1)
			sm.result[index] = ch
		}
		sm.mu.Unlock()

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

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("MOVE Operation Received at server %d: %v, %d", sm.me, args.ClientId, args.OperationId)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		DPrintf("%d is not the leader!", sm.me)
		reply.WrongLeader = true
		return
	} else {
		command := Op{}
		command.GID = args.GID
		command.Shard = args.Shard
		command.OperationId = args.OperationId
		command.ClientId = args.ClientId
		command.Operation = MOVE

		index, _, isLeader := sm.rf.Start(command)
		if !isLeader {
			DPrintf("%d is not the leader while Start!", sm.me)
			reply.WrongLeader = true
			return
		}
		sm.mu.Lock()
		ch, ok := sm.result[index]
		DPrintf("server %d move %v %d check index %d, found? %v", sm.me, args.ClientId, args.OperationId, index, ok)
		if !ok {
			ch = make(chan Result, 1)
			sm.result[index] = ch
		}
		sm.mu.Unlock()

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

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("QUERY Operation Received at server %d: %v, %d", sm.me, args.ClientId, args.OperationId)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		DPrintf("%d is not the leader!", sm.me)
		reply.WrongLeader = true
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
		command := Op{}
		command.Num = args.Num
		command.ClientId = args.ClientId
		command.OperationId = args.OperationId
		command.Operation = QUERY

		index, _, isLeader := sm.rf.Start(command)
		if !isLeader {
			reply.WrongLeader = true
			return
		}
		sm.mu.Lock()
		ch, ok := sm.result[index]
		DPrintf("server %d query %v %d check index %d, found? %v", sm.me, args.ClientId, args.OperationId, index, ok)
		if !ok {
			ch = make(chan Result, 1)
			sm.result[index] = ch
		}
		sm.mu.Unlock()

		select {
		case result := <- ch:
			if result.ClientId == args.ClientId && result.OperationId == args.OperationId {
				reply.WrongLeader = false
				reply.Config = result.Config
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
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.maxraftstate = -1
	//kv.getOpIdStorage = make(map[int64]GetReply)
	//kv.putAppendOpIdStorage = make(map[int64]PutAppendReply)
	sm.historyRecord = make(map[int64]int)
	sm.result = make(map[int]chan Result)
	go sm.UpdateStorage()

	return sm
}
