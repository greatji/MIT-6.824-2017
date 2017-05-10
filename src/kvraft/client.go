package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	lastLeader int
	OperationId int
	ClientId int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeader = 0
	ck.OperationId = 0
	ck.ClientId = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{Key : key, ClientId : ck.ClientId, OperationId: ck.OperationId}
	ck.OperationId ++
	reply := GetReply{}
	id := ck.lastLeader
	ok := ck.servers[id].Call("RaftKV.Get", &args, &reply)
	DPrintf("Client GET (%v, %d) operation, result is %v, %v", args.ClientId, args.OperationId, ok, reply.WrongLeader)
	for !ok || reply.WrongLeader {
		id = (id + 1) % len(ck.servers)
		reply = GetReply{}
		ok = ck.servers[id].Call("RaftKV.Get", &args, &reply)
		DPrintf("Client GET (%v, %d) operation, result is %v, %v", args.ClientId, args.OperationId, ok, reply.WrongLeader)
	}
	ck.lastLeader = id
	DPrintf("Client GET operation (%v, %d) completed", args.ClientId, args.OperationId)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.ClientId, OperationId: ck.OperationId}
	ck.OperationId ++
	id := ck.lastLeader
	reply := PutAppendReply{}
	ok := ck.servers[id].Call("RaftKV.PutAppend", &args, &reply)
	DPrintf("Client %v operation (%v, %d), result is %v, %v", op, args.ClientId, args.OperationId, ok, reply.WrongLeader)
	for !ok || reply.WrongLeader {
		id = (id + 1) % len(ck.servers)
		reply = PutAppendReply{}
		ok = ck.servers[id].Call("RaftKV.PutAppend", &args, &reply)
		DPrintf("Client %v operation (%v, %d), result is %v, %v", op, args.ClientId, args.OperationId, ok, reply.WrongLeader)
	}
	DPrintf("Client %v operation (%v %d) completed", op, args.ClientId, args.OperationId)
	ck.lastLeader = id
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
