package shardkv
import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotReady = "ErrNotReady"
	ErrTimeOut = "ErrTimeOut"
	ErrExpiredQuery = "ErrExpiredQuery"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OperationId	int
	ClientId	int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OperationId	int
	ClientId	int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type ShardsArgs struct {
	ShardsRequired	[]int
	ConfigNum	int
}

type ShardsReply struct {
	Kvstore		map[string]string
	History		map[int64]int
	Err		Err
	ConfigNum	int
	WrongLeader	bool
}

type ConfigArgs struct {
	ConfigNum	int
	Gid 		int
	Shards		[shardmaster.NShards]int
}

type ConfigReply struct {}