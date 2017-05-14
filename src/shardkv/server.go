package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"encoding/gob"
	"log"
	"bytes"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


const (
	PUT = "Put"
	APPEND = "Append"
	GET = "Get"
	ADD_SHARDS = "GET_SHARDS"
	CONF_CHANGE = "CONF_CHANGE"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation	string
	Key		string
	Value		string
	OperationId	int
	ClientId	int64
	NewConf		shardmaster.Config
	KvStore		map[string]string
	History		map[int64]int
}

type ShardKV struct {
	mu           	sync.Mutex
	me           	int
	rf           	*raft.Raft
	applyCh      	chan raft.ApplyMsg
	make_end     	func(string) *labrpc.ClientEnd
	gid          	int
	masters      	[]*labrpc.ClientEnd
	maxraftstate 	int // snapshot if log grows this big

	// Your definitions here.
	mck		* shardmaster.Clerk // shardmaster client
	currentConfig 	shardmaster.Config

	kvStorage 	map[string]string
	historyRecord	map[int64]int
	result 		map[int]chan Result
}

type Result struct {
	OperationId	int
	ClientId	int64
	Value 		string
	Error 		Err
	Operation	string
}

func (kv *ShardKV) CheckDuplicated(clientId int64, operationId int) bool {
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

func (kv *ShardKV) UpdateStorage() {
	for true {
		a := <- kv.applyCh
		DPrintf("Kvserver #%d-%d, Get message", kv.gid, kv.me)
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

		log := a.Command.(Op)
		kv.mu.Lock()
		//DPrintf("lock")
		DPrintf("commited at server %d-%d: %s, %v, %d, %s, %s", kv.gid, kv.me, log.Value, log.ClientId, log.OperationId, log.Key, log.Operation)
		var result Result
		result.OperationId = log.OperationId
		result.ClientId = log.ClientId
		if !kv.CheckDuplicated(log.ClientId, log.OperationId) && log.Operation == APPEND {
			DPrintf("Server #%d-%d Append Commited: shard: %d, kv.currentConfig.Shards[key2shard(log.Key)]: %d", kv.gid, kv.me, key2shard(log.Key), kv.currentConfig.Shards[key2shard(log.Key)])
			if kv.currentConfig.Shards[key2shard(log.Key)] != kv.gid {
				result.Error = ErrWrongGroup
			} else {
				v, ok := kv.kvStorage[log.Key]
				if ok {
					kv.kvStorage[log.Key] = v + log.Value
				} else {
					kv.kvStorage[log.Key] = log.Value
				}
				kv.historyRecord[log.ClientId] = log.OperationId
				result.Error = OK
			}
		} else if !kv.CheckDuplicated(log.ClientId, log.OperationId) && log.Operation == PUT {
			DPrintf("Server #%d-%d Put Commited: shard: %d, kv.currentConfig.Shards[key2shard(log.Key)]: %d", kv.gid, kv.me, key2shard(log.Key), kv.currentConfig.Shards[key2shard(log.Key)])
			if kv.currentConfig.Shards[key2shard(log.Key)] != kv.gid {
				result.Error = ErrWrongGroup
			} else {
				kv.kvStorage[log.Key] = log.Value
				kv.historyRecord[log.ClientId] = log.OperationId
				result.Error = OK
			}
		} else if log.Operation == GET {
			DPrintf("Server #%d-%d Get Commited: shard: %d, kv.currentConfig.Shards[key2shard(log.Key)]: %d", kv.gid, kv.me, key2shard(log.Key), kv.currentConfig.Shards[key2shard(log.Key)])
			if kv.currentConfig.Shards[key2shard(log.Key)] != kv.gid {
				result.Error = ErrWrongGroup
			} else {
				value, ok := kv.kvStorage[log.Key]
				if ok {
					result.Error = OK
					result.Value = value
				} else {
					result.Value = ""
					result.Error = ErrNoKey
				}
				if !kv.CheckDuplicated(log.ClientId, log.OperationId) {
					kv.historyRecord[log.ClientId] = log.OperationId
				}
			}
		} else if log.Operation == ADD_SHARDS {
			for k, v := range log.KvStore {
				kv.kvStorage[k] = v
			}
			for k, v := range log.History {
				value, ok := kv.historyRecord[k]
				if ok {
					if value < v {
						kv.historyRecord[k] = v
					}
				} else {
					kv.historyRecord[k] = v
				}
			}
		} else if log.Operation == CONF_CHANGE {
			result.Operation = CONF_CHANGE
			DPrintf("CONF_CHANGE Committed at Server #%d-%d, logNewConf: %v",kv.gid, kv.me, log.NewConf)
			if log.NewConf.Num > kv.currentConfig.Num {
				kv.currentConfig = log.NewConf
			}

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

func(kv *ShardKV) AnswerShards(args *ShardsArgs, reply *ShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	ShardsReqired := make(map[int]bool)
	for _, v := range args.ShardsRequired {
		ShardsReqired[v] = true
	}
	reply.Kvstore = make(map[string]string)
	reply.History = make(map[int64]int)
	for k, v := range kv.kvStorage {
		if _, ok := ShardsReqired[key2shard(k)]; ok {
			reply.Kvstore[k] = v
		}
	}
	for k, v := range kv.historyRecord {
		reply.History[k] = v
	}

	if kv.currentConfig.Num == args.ConfigNum {
		DPrintf("Sender will become next configuration after this reply, I can not serve the shards the sender will serve")
		for _, v := range args.ShardsRequired {
			if kv.currentConfig.Shards[v] == kv.gid {
				kv.currentConfig.Shards[v] = 0
			}
		}
	}

	reply.Err = OK
	reply.WrongLeader = false
	reply.ConfigNum = args.ConfigNum

	return
}

func (kv *ShardKV) SendRequireShards(gid int, args *ShardsArgs, reply *ShardsReply) bool {
	if len(kv.currentConfig.Groups[gid]) == 0 {
		return true
	}
	for _, v := range kv.currentConfig.Groups[gid] {
		DPrintf("Server #%d-%d SendRequireShards to Server #%d: ConfigNum: %d, ShardsRequired: %v", kv.gid, kv.me, v, args.ConfigNum, args.ShardsRequired)
		ok := kv.make_end(v).Call("ShardKV.AnswerShards", args, reply)
		DPrintf("Server #%d-%d SendRequireShards to Server #%d: OK: %v, Err: %s, History: %v, Kvstore: %v", kv.gid, kv.me, v, ok, reply.Err, reply.History, reply.Kvstore)
		if ok && reply.Err == OK {
			return true
		} else if ok && reply.Err == ErrNotReady {
			return false
		}
	}
	return false
}

func (kv *ShardKV) RequireShards(newConfig shardmaster.Config) bool {

	kv.mu.Lock()
	shardsRequired := make(map[int][]int)
	for shardIndex, gid := range newConfig.Shards {
		if gid == kv.gid && kv.currentConfig.Shards[shardIndex] != kv.gid && kv.currentConfig.Shards[shardIndex] != 0{
			value, ok := shardsRequired[kv.currentConfig.Shards[shardIndex]]
			if ok {
				shardsRequired[kv.currentConfig.Shards[shardIndex]] = append(value, shardIndex)
			} else {
				shardsRequired[kv.currentConfig.Shards[shardIndex]] = append(make([]int, 0), shardIndex)
			}
		}
	}
	kv.mu.Unlock()

	res := true
	var wait sync.WaitGroup
	DPrintf("Server #%d-%d require shards: ShardsRequired: %v", kv.gid, kv.me, shardsRequired)
	for k, v := range shardsRequired {
		// send group k with v requirements
		wait.Add(1)
		go func() {
			defer wait.Done()
			args := ShardsArgs{}
			args.ConfigNum = kv.currentConfig.Num
			args.ShardsRequired = v
			reply := ShardsReply{}
			ok := kv.SendRequireShards(k, &args, &reply)
			if !ok {
				res = false
			} else {
				operation := Op{}
				operation.Operation = ADD_SHARDS
				operation.History = make(map[int64]int)
				operation.KvStore = make(map[string]string)
				for k, v := range reply.Kvstore {
					operation.KvStore[k] = v
				}
				for k, v := range reply.History {
					value, ok := operation.History[k]
					if ok {
						if value < v {
							operation.History[k] = v
						}
					} else {
						operation.History[k] = v
					}
				}
				kv.rf.Start(operation)
			}
		}()
	}

	wait.Wait()
	return res
}

func (kv *ShardKV) CheckMigration() {
	for true {
		if _, isLeader := kv.rf.GetState(); isLeader {
			// configuration change
			newConfig := kv.mck.Query(-1)
			DPrintf("Server #%d-%d is leader, currentCfg: %v, newCfg: %v", kv.gid, kv.me, kv.currentConfig, newConfig)
			for num := kv.currentConfig.Num + 1; num <= newConfig.Num; num ++ {
				newConfig := kv.mck.Query(num)
				ok := kv.RequireShards(newConfig)
				if !ok {
					break
				}
				operation := Op{}
				operation.Operation = CONF_CHANGE
				operation.NewConf = newConfig
				index, _, isLeader := kv.rf.Start(operation)
				if !isLeader {
					break
				}
				kv.mu.Lock()
				ch, ok := kv.result[index]
				if !ok {
					ch = make(chan Result, 1)
					kv.result[index] = ch
				}
				kv.mu.Unlock()

				select {
				case result := <- ch:
					if result.Operation != CONF_CHANGE {
						break
					}
				case <- time.After(1000 * time.Millisecond): break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Get Operation Received at server %d-%d: %s, %v, %d", kv.gid, kv.me, args.Key, args.ClientId, args.OperationId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("%d-%d is not the leader!", kv.gid, kv.me)
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
		DPrintf("server %d-%d putappend %v %d check index %d, found? %v", kv.gid, kv.me, args.ClientId, args.OperationId, index, ok)
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
				reply.Err = result.Error
				return
			}
		case <- time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%s Operation Received at server %d-%d: %v, %d, %v, %s", args.Op, kv.gid, kv.me, args.ClientId, args.OperationId, args.Key, args.Value)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("%d-%d is not the leader!", kv.gid, kv.me)
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
		DPrintf("server %d-%d putappend %v %d check index %d, found? %v", kv.gid, kv.me, args.ClientId, args.OperationId, index, ok)
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
				reply.Err = result.Error
				return
			}
		case <- time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.currentConfig = kv.mck.Query(-1)

	kv.kvStorage = make(map[string]string)
	kv.historyRecord = make(map[int64]int)
	kv.result = make(map[int]chan Result)
	go kv.UpdateStorage()
	go kv.CheckMigration()

	return kv
}
