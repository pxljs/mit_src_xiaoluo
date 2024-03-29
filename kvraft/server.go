package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KvMap  map[string]string //存储键值对
	SeqMap map[int64]bool    //存储请求id
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, _, isleader := kv.rf.Start(args)
	if !isleader {
		reply.Err = "Not Leader"
		return
	}

	for {
		select {
		case pojo := <-kv.applyCh:
			fmt.Println(pojo)
			if _, exists := kv.SeqMap[args.SeqID]; exists {
				fmt.Println("重复的请求")
				return
			} else {
				kv.SeqMap[args.SeqID] = true
			}
			if _, exists := kv.KvMap[args.Key]; exists {
				reply.Value = kv.KvMap[args.Key]
				fmt.Printf("成功从KvMap中获取键：'%+v'对应的值：'%+v'\n", args.Key, reply.Value)
			} else {
				fmt.Printf("键 '%+v' 不存在于DataMap中\n", args.Key)
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.rf.Start(args)

	_, _, isleader := kv.rf.Start(args)
	if !isleader {
		reply.Err = "Not Leader"
		return
	}

	for {
		select {
		case pojo := <-kv.applyCh:
			fmt.Println(pojo)
			if _, exists := kv.SeqMap[args.SeqID]; exists {
				fmt.Println("重复的请求")
				return
			} else {
				kv.SeqMap[args.SeqID] = true
			}
			if args.Op == "Put" {
				kv.KvMap[args.Key] = args.Value
				return
			} else {
				kv.KvMap[args.Key] += args.Value
				return
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.KvMap = make(map[string]string)
	kv.SeqMap = make(map[int64]bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	return kv
}
