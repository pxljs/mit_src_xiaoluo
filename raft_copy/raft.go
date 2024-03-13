package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Term             int64  // 维护的任期，初始化的时候是1
	Role             int32  // 角色  1-follower  2-candidate  3-leader
	VotedFor         int32  // 投票给了谁 编号 [0,n)， 初始化时为-1
	PeersVoteGranted []bool // 被投票的结果,leader才有

	RequestVoteTimeTicker          *time.Ticker // 投票循环计时器，follower和candidate才有
	RequestVoteDuration            time.Duration
	RequestAppendEntriesTimeTicker *time.Ticker // rpc循环计时器，leader才有
	RequestAppendEntriesDuration   time.Duration

	Log         []LogEntry // 日志数组   在语义上第一个下标为1而不是0
	CommitIndex int        // 已经提交的最大的日志index， 初始化为0

	ApplyCh     chan ApplyMsg // 检测程序所用，提交之后的日志发送到这里
	LastApplied int           // 最后一个被应用到[完成提交]状态机的日志下标, 实验2B用不到

	NextIndex  []int // leader才有意义。对于各个raft节点，下一个需要接收的日志条目的索引，初始化为自己最后一个log的下标+1
	MatchIndex []int // leader才有意义。对于各个raft节点，已经复制过去的最高的日志下标【正常是从1开始，所以这里初始化是0】

	// 实验2D
	LastIncludedTerm  int64 // 最后快照保存的term
	LastIncludedIndex int   // 最后快照保存的index, 初始化为0. 之后正常从1开始
}

const (
	// RoleFollower 角色  1-follower  2-candidate  3-leader
	RoleFollower            = 1
	RoleCandidate           = 2
	RoleLeader              = 3
	InitVoteFor             = -1                     // 初始化投票为空
	InitTerm                = 1                      // 初始化任期
	BaseRPCCyclePeriod      = 40 * time.Millisecond  // 一轮周期基线，每个实例在这个基础上新增 0~ RPCRandomPeriod 毫秒的随机值
	BaseElectionCyclePeriod = 300 * time.Millisecond // 一轮周期基线，每个实例在这个基础上新增 0~ ElectionRandomPeriod 毫秒的随机值

	RPCRandomPeriod      = 10
	ElectionRandomPeriod = 100
)

// GlobalID 全局自增ID，需要原子性自增，用于debug
var GlobalID = int64(100)

type LogEntry struct {
	Term    int64       // 任期
	Index   int         // 在日志list中的索引号，从1开始
	Command interface{} // 具体的数据，在这里面就是命令
	ID      int64       // 全局唯一id
}

// AppendEntriesRequest 只有leader才能发送的心跳包
type AppendEntriesRequest struct {
	Term         int64 // 自己这里维护的任期
	ServerNumber int32 // 我是哪台机器, 编号 [0,n)   实际上就是leader的编号

	// 2B、2C
	PrevLogIndex      int        // Leader节点认为该Follower节点已有的上一条日志条目的索引,从0开始
	PrevLogTerm       int64      // PrevLogIndex 的任期
	LeaderCommitIndex int        // 自己的 commitIndex 值
	Entries           []LogEntry // 要复制过去的日志
}

// AppendEntriesReply 收到心跳后的回复结构体
type AppendEntriesReply struct {
	Term         int64 // 自己这里维护的任期
	ServerNumber int32 // 回复一下自己是哪台机器, 编号 [0,n)

	// 2B、2C
	Success    bool // 如果跟随者包含与prevLogIndex和prevLogTerm匹配的条目，则为true。
	MatchIndex int  // 告诉leader最后一次复制完的日志index
	HasReplica bool // follower是否复制了日志, 仅用于debug打印
}

// SnapshotReq 实验2D，照着论文图13实现
type SnapshotReq struct {
	Term              int64 // 自己这里维护的任期
	ServerNumber      int32 // 回复一下自己是哪台机器, 编号 [0,n)
	LastIncludedTerm  int64 // 最后快照保存的term
	LastIncludedIndex int   // 最后快照保存的index, 初始化为0. 之后正常从1开始
	Done              bool
	// 实验2D的说明中，要求发送整个快照，而不是分隔后的。所以下面两个字段不用。
	Offset int
	Data   []byte
}

// SnapshotResp 实验2D，照着论文图13实现
type SnapshotResp struct {
	Term         int64 // 自己这里维护的任期
	ServerNumber int32 // 回复一下自己是哪台机器, 编号 [0,n)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.Term), rf.Role == RoleLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.Term)
	_ = e.Encode(rf.VotedFor)
	_ = e.Encode(rf.Log)
	_ = e.Encode(rf.LastIncludedIndex)
	_ = e.Encode(rf.LastIncludedTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.Term); err != nil {
		panic("Decode失败 Term" + err.Error())
	}
	if err := d.Decode(&rf.VotedFor); err != nil {
		panic("Decode失败 VotedFor" + err.Error())
	}
	if err := d.Decode(&rf.Log); err != nil {
		panic("Decode失败 Log" + err.Error())
	}
	if err := d.Decode(&rf.LastIncludedIndex); err != nil {
		panic("Decode失败 LastIncludedIndex" + err.Error())
	}
	if err := d.Decode(&rf.LastIncludedTerm); err != nil {
		panic("Decode失败 LastIncludedTerm" + err.Error())
	}
	// 这里要更新一下 CommitIndex 不然初始化为0会导致对 old CommitIndex 的判定出问题
	rf.CommitIndex = rf.LastIncludedIndex
	Info("%d号机器Decode成功————————————————————————————", rf.me)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// index参数是当前CommandIndex， snapShot是从0到CommandIndex之间的日志
	// 实验有提示，不让加锁
	if index <= rf.LastIncludedIndex {
		return
	}
	Info("%d号机器执行Snapshot"+fmt.Sprint(" 老新LastIncludedIndex", rf.LastIncludedIndex, "-", index), rf.me)
	// 删除自身所保存的一些日志
	rf.LastIncludedTerm = rf.Log[index-1-rf.LastIncludedIndex].Term
	rf.Log = rf.Log[index-rf.LastIncludedIndex:]
	rf.LastIncludedIndex = index
	// 先保存到 state 中，再保存 snapShot
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

// RequestVoteArgs 发起投票请求的结构体
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64 // 自己这里维护的任期
	ServerNumber int32 // 我是哪台机器, 编号 [0,n)
	LastLogIndex int   // 所持有的最后一条日志记录的 index, 从1开始
	LastLogTerm  int64 // LastLogIndex 的任期
}

// RequestVoteReply 回复投票请求的结构体
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Agree        bool  // 同意还是拒绝
	Term         int64 // 回复一下自己维护的任期
	ServerNumber int32 // 回复一下自己是哪台机器, 编号 [0,n)
}

// ————————————————————————————————   选举相关   ————————————————————————————————————————

// RequestVote 处理别人的选举请求
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term, reply.ServerNumber = rf.Term, int32(rf.me)
	// 自己的话直接跳过
	if int(args.ServerNumber) == rf.me {
		rf.VotedFor = args.ServerNumber
		reply.Agree = true
		return
	}
	//fmt.Print(time.Now().Format("2006/01/02 15:04:05.000"), "  ", rf.me, " 号机器收到 ", args.ServerNumber, " 号机器的投票请求, 自己的任期是 ", rf.Term, " 请求中的任期是", args.Term, " 自己的VotedFor", rf.VotedFor, " LastLogIndex:", args.LastLogIndex, " LastLogTerm:", args.LastLogTerm)
	// 论文原文 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.Term {
		rf.convert2Follower(args.Term)
		reply.Term = rf.Term
	}
	// 当且仅当满足一下条件，才赞成投票。
	// 论文原文 If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	logOld := false
	if rf.Term == args.Term && (rf.VotedFor == InitVoteFor || rf.VotedFor == args.ServerNumber) {
		// 投票发起人的日志得比自己的新
		// 论文中对"新"的定义：任期号不同，则任期号大的比较新。任期号相同，索引值大的（日志较长的）比较新
		logOld = len(rf.Log) != 0 && (rf.Log[len(rf.Log)-1].Term > args.LastLogTerm ||
			rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && len(rf.Log)+rf.LastIncludedIndex > args.LastLogIndex)
		if !logOld {
			reply.Agree = true
			rf.Role = RoleFollower
			rf.VotedFor = args.ServerNumber
			rf.RequestVoteTimeTicker.Reset(rf.RequestVoteDuration)
		}
	}
	//fmt.Println(" 结果是:", reply.Agree, " logOld:", logOld, "log:", fmt.Sprintf("%+v", rf.Log))
}

// AsyncBatchSendRequestVote 非领导者并行发送投票请求，收到响应后进行处理
func (rf *Raft) AsyncBatchSendRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for index, _ := range rf.peers {
		args := &RequestVoteArgs{
			Term:         rf.Term,
			ServerNumber: int32(rf.me),
		}
		if len(rf.Log) != 0 {
			args.LastLogIndex = len(rf.Log) + rf.LastIncludedIndex
			args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
		}
		reply := &RequestVoteReply{}
		//fmt.Println(time.Now().Format("2006/01/02 15:04:05.000"), "  ", rf.me, "号机器发送选主请求, 发给", index, " 号  自己的信息是:", fmt.Sprintf("%+v", *args))
		go func(i int) {
			if flag := rf.sendRequestVote(i, args, reply); !flag {
				//  网络原因，需要重发，这里先不实现
				//Error(fmt.Sprint("网络原因【选举】发送不成功！！！", rf.me, "号机器发送选举请求给", i, "号机器没有成功, 任期为 ", rf.Term))
			} else {
				rf.HandleRequestVoteResp(args, reply)
			}
		}(index)
	}
}

// 请求投票的req被返回了，处理一下
func (rf *Raft) HandleRequestVoteResp(req *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if rf.Term < reply.Term {
		// 响应的任期大于自己的，立刻重置自己的任期和计时器, 并降级为 follower
		Warning(fmt.Sprint(" RV响应的任期大于自己的，立刻重置自己的任期和计时器, 并降级为 follower", rf.me, "号机器，任期为 ", rf.Term, " 响应任期为", reply.Term))
		rf.convert2Follower(reply.Term)
		return
	}
	//fmt.Println(time.Now().Format("2006/01/02 15:04:05.000"), "  ", rf.me, "号机器收到 ", reply.ServerNumber, " 号机器的投票回复，", reply.Agree, " 自己的Role:", rf.Role)
	if rf.Term == reply.Term && rf.Role == RoleCandidate {
		rf.PeersVoteGranted[reply.ServerNumber] = reply.Agree
		// 看看是不是同意一半以上了
		if checkBoolListHalfTrue(rf.PeersVoteGranted) {
			// 切换成ld角色，并做一些操作
			rf.Role = RoleLeader
			rf.RequestVoteTimeTicker.Reset(rf.RequestVoteDuration)
			// 要更新一下NextIndex数组，设置为自身的日志的最后一项的索引值加一
			// 这是因为新的ld尚未知道跟随者的日志与自己的日志在哪一点开始不一致，所以初始化为自己的日志的下一项是合理的
			// 之后心跳如果被拒绝，ld会将对应跟随者的NextIndex递减并重发appendEntries请求，这个过程会不断重复直到找到两者日志一致的点
			for i := range rf.NextIndex {
				rf.NextIndex[i] = len(rf.Log) + 1 + rf.LastIncludedIndex
				rf.MatchIndex[i] = 0
			}
			Success(fmt.Sprint(time.Now().Format("2006/01/02 15:04:05.000"), "  ", rf.me, "号机器升级成 leader,任期为", rf.Term))
			//  根据论文，应该立马添加一个 no-op 日志。因为论文图 8 说明即使某日志被复制到多数节点也不代表它被提交。但这里不需要添加，否则检测不过
			// 启动 rpc 后台协程
			go rf.backupGroundRPCCycle()
		}
	}
}

// 启动心跳后台任务。只有 ledaer 才会发心跳
func (rf *Raft) backupGroundRPCCycle() {
	// 一直循环下去，直到实例退出
	for atomic.LoadInt32(&rf.dead) != 1 && atomic.LoadInt32(&rf.Role) == RoleLeader {
		select {
		case <-rf.RequestAppendEntriesTimeTicker.C:
			// 根据角色不同，去发rpc请求
			switch atomic.LoadInt32(&rf.Role) {
			case RoleLeader:
				rf.AsyncBatchSendRequestAppendEntries()
			default:
				// 不是leader了，返回吧
				return
			}
		}
	}
}

// AsyncBatchSendRequestAppendEntries 异步并行发送多个心跳包
func (rf *Raft) AsyncBatchSendRequestAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Role != RoleLeader {
		return
	}
	for index, _ := range rf.peers {
		if index == rf.me {
			continue // 跳过自己
		}
		args := &AppendEntriesRequest{
			Term:              rf.Term,
			ServerNumber:      int32(rf.me),
			LeaderCommitIndex: rf.CommitIndex,
		}
		// 如果 NextIndex ~ lastIndex 中有已经被snapShot过的，那改成发snapShot
		if rf.NextIndex[index] <= rf.LastIncludedIndex {
			resp := &SnapshotResp{}
			req := &SnapshotReq{
				Term:              rf.Term,
				ServerNumber:      int32(rf.me),
				LastIncludedTerm:  rf.LastIncludedTerm,
				LastIncludedIndex: rf.LastIncludedIndex,
				Data:              rf.persister.ReadSnapshot(),
			}
			go func(i int) {
				Info("%d号leader发送snapShot请求给%d号,  req.LastIncludedIndex:%+v", req.ServerNumber, i, req.LastIncludedIndex)
				if flag := rf.sendRPCSnapshotRequest(i, req, resp); !flag {
					// 网络问题，先忽略
				} else {
					// 处理响应
					rf.HandleSnapShotResp(req, resp)
				}
			}(index)
			// 继续外层循环
			continue
		}
		// PrevLogIndex 是 leader 认为该 follower 上一次发送过的 log 的 index， 和 nextIndex 差 1
		args.PrevLogIndex = rf.NextIndex[index] - 1
		if args.PrevLogIndex-1 >= 0+rf.LastIncludedIndex && len(rf.Log)+rf.LastIncludedIndex >= args.PrevLogIndex {
			args.PrevLogTerm = rf.Log[args.PrevLogIndex-1-rf.LastIncludedIndex].Term
		}
		lastIndex := MathMin(args.PrevLogIndex+100, len(rf.Log)+rf.LastIncludedIndex)
		// 只复制NextIndex这一个。 每次心跳只复制一个 [追日志进度的时候可能有多个日志复制，保持原子性，这里忽略]
		// 复制多个，不然 图8 检测会因原子性失败
		for i := args.PrevLogIndex; i < lastIndex; i++ {
			args.Entries = append(args.Entries, rf.Log[i-rf.LastIncludedIndex])
		}
		reply := &AppendEntriesReply{}
		go func(i int) {
			//Trace(fmt.Sprint(rf.me, "号机器开始发送心跳给", i, "号机器, 任期为 ", rf.Term, "  req为", fmt.Sprintf("%+v %s", *args, logPrint)))
			if flag := rf.sendRPCAppendEntriesRequest(i, args, reply); !flag {
				//  网络原因，需要重发
				//Error(fmt.Sprint("网络原因【心跳】发送不成功！！！", rf.me, "号机器发送心跳给", i, "号机器没有成功, 任期为 ", rf.Term))
			} else {
				rf.HandleAppendEntriesResp(args, reply)
			}
		}(index)
	}
}

// AppendEntries 收到心跳包，如何回应
func (rf *Raft) AppendEntries(req *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = true
	reply.ServerNumber = int32(rf.me)
	reply.Term = rf.Term
	reply.MatchIndex = 0 + rf.LastIncludedIndex
	// 是自己的话直接返回
	if req.ServerNumber == int32(rf.me) {
		return
	}
	// 先看下任期是否还一样
	// 1. Reply false if term < currentTerm (§5.1)
	if rf.Term > req.Term {
		// 源leader任期更小，说明老leader过期了，告诉他自己的任期
		Error("源leader编号%d任期更小 请求任期 %d 自己的任期 %d, 直接返回", req.ServerNumber, req.Term, rf.Term)
		reply.Success = false
		return
	}
	// 源leader的任期比自己大，说明自己的任期落后了，需要更新
	if rf.Term < req.Term {
		rf.convert2Follower(req.Term)
	}
	// 之后一定是  req.Term == rf.Term
	rf.Role = RoleFollower
	rf.RequestVoteTimeTicker.Reset(rf.RequestVoteDuration)
	reply.Term = rf.Term

	// 如果自己日志的此下标没有，或者任期和预期的不一样，返回false
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// todo 偶现 TestSnapshotInstall2D panic 越界 index 为负数
	if req.PrevLogIndex != 0+rf.LastIncludedIndex && (len(rf.Log)+rf.LastIncludedIndex < req.PrevLogIndex || rf.Log[req.PrevLogIndex-1-rf.LastIncludedIndex].Term != req.PrevLogTerm) {
		// 找一下最后匹配到哪一个了，不然服务端不停的-1重试导致超时
		reply.MatchIndex = rf.CommitIndex
		reply.Success = false
		//Warning(fmt.Sprint(rf.me, "机器收到", req.ServerNumber, "的心跳【发生日志冲突】", " CommitIndex:", rf.CommitIndex, fmt.Sprintf(" req:%+v reply:%+v Log:%+v", *req, *reply, rf.Log)))
		return
	}

	// 如果是空心跳包，那么matchIndex设置为最后的日志index，下面 for 会修改
	reply.MatchIndex = req.PrevLogIndex
	// 如果自己的日志和req中的发生任期冲突，删除所有已有的index之后的
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for _, pojo := range req.Entries {
		reply.MatchIndex += 1
		for len(rf.Log)+rf.LastIncludedIndex >= pojo.Index && rf.Log[pojo.Index-1-rf.LastIncludedIndex].Term != pojo.Term {
			Warning(fmt.Sprint(rf.me, "机器丢弃日志，因为ld心跳中的日志", ",值为", rf.CommitIndex, fmt.Sprintf(" reply:%+v 丢弃的Log是%+v", *reply, rf.Log[pojo.Index-1-rf.LastIncludedIndex])))
			rf.Log = rf.Log[:pojo.Index-1-rf.LastIncludedIndex]
		}
	}

	// 4. Append any new entries not already in the log
	for _, pojo := range req.Entries {
		// 不要重复添加
		if len(rf.Log)+rf.LastIncludedIndex < pojo.Index || rf.Log[pojo.Index-1-rf.LastIncludedIndex].Term != pojo.Term {
			// 不应该取 req 日志中的 index， 要重新弄成自己的index
			rf.Log = append(rf.Log, LogEntry{
				Term:    pojo.Term,
				Index:   len(rf.Log) + 1 + rf.LastIncludedIndex,
				Command: pojo.Command,
				ID:      pojo.ID,
			})
			reply.HasReplica = true
		}
	}

	// 若 req中leaderCommit > 自己的commitIndex，令 commitIndex 等于 leaderCommit 和最后一个新日志记录的 index 值之间的最小值
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	commitIndexUpdate := req.LeaderCommitIndex > rf.CommitIndex && len(rf.Log) != 0
	oldCommitIndex := rf.CommitIndex
	if commitIndexUpdate {
		rf.CommitIndex = MathMin(req.LeaderCommitIndex, rf.Log[len(rf.Log)-1].Index)
	}

	//Trace(fmt.Sprint(rf.me, "机器收到", req.ServerNumber, "的心跳", ",commitIndex是否更新", commitIndexUpdate, ",值从", oldCommitIndex, "变为", rf.CommitIndex, fmt.Sprintf(" reply:%+v Log:%+v", *reply, rf.Log)))
	if commitIndexUpdate {
		// 当 CommitIndex 更新时，相当于提交，需要给检测程序发送
		for i := oldCommitIndex; i <= rf.CommitIndex-1; i++ {
			rf.ApplyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i-rf.LastIncludedIndex].Command,
				CommandIndex: rf.Log[i-rf.LastIncludedIndex].Index,
			}
			rf.LastApplied = rf.CommitIndex
		}
	}
}

// HandleAppendEntriesResp 心跳 req 被返回了，处理一下
func (rf *Raft) HandleAppendEntriesResp(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if rf.Term < reply.Term {
		fmt.Println(time.Now().Format("2006/01/02 15:04:05.000"), "  ", rf.me, " 号机器延迟收到", reply.ServerNumber, "心跳的回复，退出leader状态, 任期从", rf.Term, " 到 ", reply.Term)
		rf.convert2Follower(reply.Term)
		return
	}
	if rf.Role != RoleLeader {
		return
	}
	// 日志复制返回成功
	if reply.Success {
		rf.MatchIndex[reply.ServerNumber] = MathMax(reply.MatchIndex, rf.MatchIndex[reply.ServerNumber])
		rf.NextIndex[reply.ServerNumber] = reply.MatchIndex + 1
	} else if rf.Term == reply.Term {
		// 如果leader发送的 AppendEntries 因为日志不一致而失败，减少 nextIndex 并重试. 根据follower的实现中，失败原因只有两种，一个是term不一致一个是日志不一致
		oldNextIndex := rf.NextIndex[reply.ServerNumber]
		// 下面是普通的情况，但需要优化。因为如果落下的很多，那么尝试对齐的次数太多导致长期达不到一致，导致测试TestFigure8Unreliable2C不通过
		//rf.NextIndex[reply.ServerNumber] = MathMax(1, rf.NextIndex[reply.ServerNumber]-1)
		rf.NextIndex[reply.ServerNumber] = MathMax(1, reply.MatchIndex+1)
		Warning(fmt.Sprint(rf.me, "号ld减少%d号机器的nextIndex从", oldNextIndex, "到", rf.NextIndex[reply.ServerNumber], " reply.MatchIndex:", reply.MatchIndex), reply.ServerNumber)
	}
	// todo 所以新上任的 Leader 在接受客户写入命令前先提交一个 no-op（空命令），携带自己任期号的多数节点。本实验中不需要实现
	// 论文图 8 说明即使某日志被复制到多数节点也不代表它被提交。 所以 Raft 对日志提交条件增加一个额外限制：Leader 在至少有一条当前任期的日志被复制到大多数节点上。
	commitFlag := false
	oldCommitIndex := rf.CommitIndex
	matchIndexes := make([]int, 0)
	for _, index := range rf.MatchIndex {
		matchIndexes = append(matchIndexes, index)
	}
	sort.Ints(matchIndexes)
	n := matchIndexes[len(rf.peers)/2+1]
	if n > 0+rf.LastIncludedIndex && len(rf.Log)+rf.LastIncludedIndex >= n && rf.Log[n-1-rf.LastIncludedIndex].Term == rf.Term && rf.CommitIndex < n {
		rf.CommitIndex = n
		commitFlag = true
	}
	//fmt.Println(time.Now().Format("2006/01/02 15:04:05.000"), "  心跳成功返回~~~", rf.me, "号机器发送心跳给", reply.ServerNumber, "号机器成功, 任期为 ", rf.Term, " success:", reply.Success, " follower是否有日志复制", reply.HasReplica, " 是否可以提交本条日志:", commitFlag, "  最大提交CommitIndex从", oldCommitIndex, "变为", rf.CommitIndex)
	if commitFlag {
		// 可能一次性提交多个，所以不能依赖 req 中的 entry。从上次提交到本次提交之间的log，进行提交
		for i := oldCommitIndex; i < rf.CommitIndex; i++ {
			rf.ApplyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i-rf.LastIncludedIndex].Command,
				CommandIndex: rf.Log[i-rf.LastIncludedIndex].Index,
			}
			rf.LastApplied = rf.CommitIndex
		}
	}
}

// 转换为 follower 用的
func (rf *Raft) convert2Follower(term int64) {
	rf.Role = RoleFollower
	rf.Term = term
	rf.VotedFor = InitVoteFor
}

// InstallSnapshot follower 收到 leader 发送的 snapShot 后，要安装到自己内部。 按照论文图13实现
func (rf *Raft) InstallSnapshot(req *SnapshotReq, resp *SnapshotResp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Term = rf.Term
	resp.ServerNumber = int32(rf.me)
	// 1. 如果当前任期号比快照的任期号大，拒绝安装快照
	if rf.Term > req.Term {
		Warning("%d号机器收到%d号机器的【InstallSnapshot】任期冲突，拒绝安装快照 rf.Term:%d  req.Term:%d", rf.me, req.ServerNumber, rf.Term, req.Term)
		return
	}
	// 如果快照的index比自己上次生成/接收快照的index还小，说明req快照更旧，直接返回
	if req.LastIncludedIndex <= rf.LastIncludedIndex {
		Warning("%d号机器收到%d号机器的快照的index比自己上次生成/接收快照的index还小 req.LastIncludedIndex:%d  rf.LastIncludedIndex:%d", rf.me, req.ServerNumber, req.LastIncludedIndex, rf.LastIncludedIndex)
		return
	}
	// 开始安装快照
	// 清理日志。 如果自己当前所有的日志没有 snapShot 里面的多，则可以直接丢弃所有已有日志，让 snapShot全量覆盖
	if req.LastIncludedIndex-rf.LastIncludedIndex < len(rf.Log) {
		rf.Log = rf.Log[req.LastIncludedIndex-rf.LastIncludedIndex:]
	} else {
		rf.Log = make([]LogEntry, 0)
	}

	Success("%d号机器收到%d号机器的【InstallSnapshot】删除日志和修改状态成功"+fmt.Sprint(" 老新CommitIndex", rf.CommitIndex, "-", MathMax(rf.CommitIndex, req.LastIncludedIndex),
		" 老新LastIncludedIndex", rf.LastIncludedIndex, "-", req.LastIncludedIndex, " 老新LastIncludedTerm", rf.LastIncludedTerm, "-", req.LastIncludedTerm), rf.me, req.ServerNumber)

	// 3. 修改一些状态.
	rf.CommitIndex = MathMax(rf.CommitIndex, req.LastIncludedIndex)
	rf.LastIncludedIndex = req.LastIncludedIndex
	rf.LastIncludedTerm = req.LastIncludedTerm

	// 先保存到 state 中，再保存 snapShot
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), req.Data)

	// 最后要发送给检测程序
	rf.ApplyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotTerm:  int(req.LastIncludedTerm),
		SnapshotIndex: req.LastIncludedIndex,
	}
}

// HandleSnapShotResp 处理发送快照得到的响应，模仿 HandleAppendEntriesResp 函数
func (rf *Raft) HandleSnapShotResp(req *SnapshotReq, resp *SnapshotResp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.Term < resp.Term {
		fmt.Println(time.Now().Format("2006/01/02 15:04:05.000"), "  ", rf.me, " 号机器延迟收到", resp.ServerNumber, "【快照】的回复，退出leader状态, 任期从", rf.Term, " 到 ", resp.Term)
		rf.convert2Follower(resp.Term)
		return
	}
	if rf.Role != RoleLeader {
		return
	}
	Success(fmt.Sprint(rf.me, "发送给", resp.ServerNumber, "的快照成功, 老新NextIndex", rf.NextIndex[resp.ServerNumber], "-", MathMax(req.LastIncludedIndex+1, rf.NextIndex[resp.ServerNumber]),
		" 老新MatchIndex", rf.MatchIndex[resp.ServerNumber], "-", MathMax(req.LastIncludedIndex, rf.MatchIndex[resp.ServerNumber])))
	rf.NextIndex[resp.ServerNumber] = MathMax(req.LastIncludedIndex+1, rf.NextIndex[resp.ServerNumber])
	rf.MatchIndex[resp.ServerNumber] = MathMax(req.LastIncludedIndex, rf.MatchIndex[resp.ServerNumber])
	//nxt = rf.nextIndex[id]
	//req.PrevLogTerm = reqs.LastIncludeTerm
	//req.PrevLogIndex = reqs.LastIncludeIndex

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 模仿上面，发送心跳rpc
func (rf *Raft) sendRPCAppendEntriesRequest(server int, req *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}

// 模仿上面，发送快照rpc
func (rf *Raft) sendRPCSnapshotRequest(server int, req *SnapshotReq, reply *SnapshotResp) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", req, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader = rf.GetState()
	if !isLeader {
		// 不是ld直接返回
		return index, term, isLeader
	}

	// Your code here (2B).
	if rf.killed() {
		return 0, 0, false
	}

	rf.Log = append(rf.Log, LogEntry{
		Command: command,
		Index:   len(rf.Log) + 1 + rf.LastIncludedIndex,
		Term:    rf.Term,
		ID:      atomic.AddInt64(&GlobalID, 1),
	})
	//Success(fmt.Sprint(rf.me, "号leader机器添加一个日志", fmt.Sprintf("%+v", rf.Log), " commitIndex为", rf.CommitIndex))
	//time.Sleep(30 * time.Millisecond)
	rf.persist()
	return len(rf.Log) + rf.LastIncludedIndex, int(rf.Term), rf.Role == RoleLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		// 先不考虑 rpc 的超时控制，因为 sendRequestVote 说自己考虑过了。
		case <-rf.RequestVoteTimeTicker.C:
			// 时间到了
			rf.mu.Lock()
			if rf.dead == 1 {
				return
			}
			switch rf.Role {
			case RoleFollower:
				// 转换成 candidate 并且 term+1
				rf.Role = RoleCandidate
				rf.Term++
				// 重置一下投票的结果
				rf.PeersVoteGranted = make([]bool, len(rf.peers))
				rf.PeersVoteGranted[rf.me] = true
				go rf.AsyncBatchSendRequestVote()
			case RoleCandidate:
				// term + 1 , 额外的线程去做rpc
				rf.Term++
				rf.PeersVoteGranted = make([]bool, len(rf.peers))
				rf.PeersVoteGranted[rf.me] = true
				go rf.AsyncBatchSendRequestVote()
			case RoleLeader:
				// 不用做
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 初始化
	rf.VotedFor = InitVoteFor
	rf.Term = InitTerm
	rf.Role = RoleFollower
	rf.ApplyCh = applyCh
	rf.PeersVoteGranted = make([]bool, len(rf.peers))
	rf.PeersVoteGranted[me] = true // 自己总是同意自己的
	// 需要起后台线程，用于发消息和心跳等等
	rf.RequestVoteDuration = BaseElectionCyclePeriod + time.Duration(rand.Intn(ElectionRandomPeriod))*time.Millisecond
	rf.RequestVoteTimeTicker = time.NewTicker(rf.RequestVoteDuration)
	rf.RequestAppendEntriesDuration = BaseRPCCyclePeriod + time.Duration(rand.Intn(RPCRandomPeriod))*time.Millisecond
	rf.RequestAppendEntriesTimeTicker = time.NewTicker(rf.RequestAppendEntriesDuration)
	fmt.Println(rf.me, "号机器的选举循环周期是", rf.RequestVoteDuration.Milliseconds(),
		"毫秒", "  rpc周期是", rf.RequestAppendEntriesDuration.Milliseconds(), "毫秒")

	// 虽然只有ld才有，但都先初始化一下
	rf.Log = make([]LogEntry, 0)
	rf.NextIndex = make([]int, len(rf.peers)) // leader才有。要发送给各个raft节点，下一个日志的下标，初始化为最后一个log的下标+1
	for i := range rf.NextIndex {
		rf.NextIndex[i] = rf.CommitIndex + 1
	}
	rf.MatchIndex = make([]int, len(rf.peers)) // leader才有。对于各个raft节点，已经复制过去的最高的日志下标   初始化为0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// bool 数组一半以上为 true 则返回 true
func checkBoolListHalfTrue(list []bool) (flag bool) {
	count := 0
	for _, b := range list {
		if b {
			count++
		}
	}
	return count >= len(list)/2+1
}

func MathMin(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func MathMax(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// ————————————————————————————————    用于颜色打印      ———————————————————————————————————————————————

const (
	color_red = uint8(iota + 91)
	color_green
	color_yellow
	color_blue
	color_magenta //洋红
	info          = "[INFO]"
	trac          = "[TRAC]"
	erro          = "[ERRO]"
	warn          = "[WARN]"
	succ          = "[SUCC]"
)

// see complete color rules in document in https://en.wikipedia.org/wiki/ANSI_escape_code#cite_note-ecma48-13
func Trace(format string, a ...interface{}) {
	prefix := yellow(trac)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
func Info(format string, a ...interface{}) {
	prefix := blue(info)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
func Success(format string, a ...interface{}) {
	prefix := green(succ)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
func Warning(format string, a ...interface{}) {
	prefix := magenta(warn)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
func Error(format string, a ...interface{}) {
	prefix := red(erro)
	fmt.Println(formatLog(prefix), red(fmt.Sprintf(format, a...)))
}
func red(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_red, s)
}
func green(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_green, s)
}
func yellow(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_yellow, s)
}
func blue(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_blue, s)
}
func magenta(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_magenta, s)
}
func formatLog(prefix string) string {
	return time.Now().Format("2006/01/02 15:04:05.000") + " " + prefix + " "
}
