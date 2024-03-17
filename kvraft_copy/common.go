package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrFailed      = "Failed" // 操作失败，往往是Raft内部丢弃了未提交的日志
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type CommonRequest struct {
	Key   string
	Value string
	Op    string // "Put" or "Append" or "Get"
	SeqId int64
}

type CommonResponse struct {
	Err   Err
	Value string
}
