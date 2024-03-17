package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
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
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.CommonOp(key, "", "Get")
}

func (ck *Clerk) CommonOp(key string, value string, op string) string {
	seqId := nrand()
	for {
		for _, server := range ck.servers {
			req := &CommonRequest{
				Key:   key,
				Value: value,
				Op:    op,
				SeqId: seqId,
			}
			resp := &CommonResponse{}
			if b := server.Call("KVServer.CommonOp", req, resp); !b {
				//  网络错误   util.Warning("请求 %v 服务端失败", i)
				continue
			}
			// 不是leader  或者是 raft 内部有日志冲突丢弃的情况
			if resp.Err == ErrWrongLeader || resp.Err == ErrFailed {
				continue
			} else {
				return resp.Value
			}
		}
		// 一轮循环过后，如果还是没有返回，则等待后再发起请求
		time.Sleep(50 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.CommonOp(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.CommonOp(key, value, "Append")
}
