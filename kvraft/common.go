package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	SeqID int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

var GlobalID = int64(100)

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key   string
	SeqID int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
