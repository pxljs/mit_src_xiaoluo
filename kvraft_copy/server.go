package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/util"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	OpType string
	Key    string
	Value  string
	SeqId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap     map[string]string  // kv数据库，用 map 简单表示
	seqMap    map[int64]struct{} // 请求幂等性, key是时间纳秒
	persister *raft.Persister    // 持久化功能，因为 rf 里面的那个是小写的，导致包外取不到
}

func (kv *KVServer) CommonOp(req *CommonRequest, resp *CommonResponse) {
	op := Op{
		SeqId:  req.SeqId,
		OpType: req.Op,
		Key:    req.Key,
		Value:  req.Value,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 不是leader，直接返回
		resp.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// 这里可以用超时
	for i := 0; i < 400; i++ {
		if _, ok := kv.seqMap[op.SeqId]; !ok {
			kv.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			kv.mu.Lock()
		} else {
			resp.Value = kv.kvMap[op.Key]
			kv.mu.Unlock()
			//fmt.Printf("%v机器操作返回 %+v\n", kv.me, *resp)
			return
		}
	}
	util.Error("操作超时！！！  %+v", *req)
	resp.Err = ErrFailed
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.seqMap = make(map[int64]struct{})
	kv.persister = persister
	go kv.BackGround()
	return kv
}

func (kv *KVServer) BackGround() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			// CommandValid 不用判断
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			// 幂等性
			if _, ok := kv.seqMap[op.SeqId]; !ok {
				kv.seqMap[op.SeqId] = struct{}{}
				switch op.OpType {
				case "Put":
					kv.kvMap[op.Key] = op.Value
				case "Append":
					kv.kvMap[op.Key] += op.Value
				default:
					// get 不需要管
				}
			}
			// 如果日志太多，超过了参数, 需要做一次快照，保存到持久化里面
			//if kv.maxraftstate != -1 && len(kv.persister.ReadRaftState()) > kv.maxraftstate {
			//}
			kv.mu.Unlock()
		}
	}
}
