package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"sync/atomic"
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
	req := &GetArgs{
		Key:   key,
		SeqID: atomic.AddInt64(&GlobalID, 1),
	}
	resp := &GetReply{}
	for {
		for i, _ := range ck.servers {
			b := ck.servers[i].Call("KVServer.Get", req, resp)
			if !b {
				//网络错误，需要重试
				fmt.Println("网络错误，需要重试")
			}
			if resp.Err != "" {
				fmt.Printf("%+v号server不是Leader节点\n", i)
				continue
			}
			return resp.Value
		}
		time.Sleep(time.Millisecond * 100) //暂停100ms再进行下一轮循环
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
	req := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		SeqID: atomic.AddInt64(&GlobalID, 1),
	}
	resp := &PutAppendReply{}
	for {
		for i, _ := range ck.servers {
			b := ck.servers[i].Call("KVServer.PutAppend", req, resp)
			if !b {
				//网络错误，需要重试
				fmt.Println("网络错误，需要重试")
			}
			if resp.Err != "" {
				fmt.Printf("%+v号server不是Leader节点\n", i)
				continue
			}
			return
		}
		time.Sleep(time.Millisecond * 100) //暂停100ms再进行下一轮循环
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
