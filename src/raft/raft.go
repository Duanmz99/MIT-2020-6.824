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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 定义的临时日志结构体，后续可能会更改到其他地方

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type State int

const (
	Leader    State = 0
	Candidate State = 1
	Follower  State = 2
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	currentTerm int        // 记录当前节点对应的term
	votedFor    int        // 记录当前节点投票的对象（一个term只能投一次）
	leaderId    int        // 记录当前节点的leader
	logs        []LogEntry // 日志集，包含了要执行的指令和leader转发时的log

	// Volatile state on all servers
	// 二者可能不同，lastApplied用于帮助新leader快速执行全部提交却未执行的日志
	commitIndex int // 当前已知的统计出来数量大于一半而提交的log index
	lastApplied int // 当前已知的已经执了的log index

	// Volatile state on leaders
	// 每次选举后都需要重新初始化，follower和candidate用不到，只有leader会用到
	nextIndex  []int // 记录了给每一个server要发送的下一个日志
	matchIndex []int // 记录了已经复制给server的日志里的最大index

	// Other info not in figure 2
	role State // 记录当前是三个状态的哪一个
	// 限制条件：heartbeat频率不能超过1s10次
	// electionTimeOut要尽量大于heartbeat以避免浪费时间不停重复选举，另外其不能过大以避免5s内无法选举出对象
	electionTimeOut   time.Duration // 每一个raft都有一个独特的选举时间，超过时间就会启动选举，从论文来看这个值应该是确定的
	lastActiveTime    time.Time     // 用于判断上次响应的时间，针对于follower
	lastBroadcastTime time.Time     // 用于判断上次发送心跳的时间，针对于Leader

	// 存疑部分，暂时还不知道有什么用
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
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
	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RaftNode [%d] Receive RequestVote at term %d from candidate %d term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	defer func() {
		DPrintf("RaftNode [%d] Return RequestVote at term %d from candidate %d term %d and result is %+v", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted)
	}() // 使用闭包以按照引用传递而非值传递，保证最后得到的结果能反映出来
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	// 如果请求的term小于当前的term的话直接返回错误信息
	if args.Term < rf.currentTerm {
		return
	}
	// 如果对面的term比自己的大则自动转为follower状态，并清空信息
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 包含了candidateId情况可能是为了保证丢包时可以得到相同回应
		lastLogTerm := 0
		if len(rf.logs) != 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.logs)) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			// 只有当对candidate进行投票或者接收到appendEntry时才更新时间，否则不变
			rf.lastActiveTime = time.Now()
		}
	}
	rf.persist()
	return
}

//
// appendEntries Handler
//
// current leader是个值得注意的点，后续可能需要进行修改这里，以处理多leader的情况(取决于是否出现集群迁移的情况)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RaftNode [%d] handle AppendEntries request at term %d from leader %d term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	defer func() {
		DPrintf("RaftNode [%d] handle AppendEntries request at term %d from leader %d term %d result %v", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success)
	}()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictIndex = -1
	// partA暂时用不上这里的更新，后续有需要再进行修改
	// 检验1
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leaderId = -1
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}
	// 一样的话无需像RequestVote一样进行一致性判断，直接进行状态更新即可
	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()
	// 检验figure2关于append RPC的剩下几条
	// 检验2：最后一条日志对不上的情况
	// 本地没有前一条日志
	if len(rf.logs) < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
		return
	}
	// 本地有前一条日志但是对不上
	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[args.PrevLogIndex-1].Term
		for index, _ := range rf.logs {
			if rf.logs[index].Term == reply.ConflictTerm {
				reply.ConflictIndex = index + 1
				break
			}
		}
		return
	}
	// 操作3，4
	// 传送过来的logEntries从nextIndex对应的entry开始，即Index为args.PrevLogIndex+1的日志
	for i, logEntry := range args.Entries {
		// index为当前比较的日志的逻辑index而非数组下标,是否需要+1存疑
		index := args.PrevLogIndex + i + 1
		if index > len(rf.logs) {
			rf.logs = append(rf.logs, logEntry)
		} else {
			if rf.logs[index-1] != logEntry {
				rf.logs = rf.logs[:index-1]
				rf.logs = append(rf.logs, logEntry)
			}
		}
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs) {
			rf.commitIndex = len(rf.logs)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true
	return
}

//
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
//

// 发送投票请求的RPC调用
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 发送appendEntry的RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 后续对架构修改，避免这种代码的出现
func (rf *Raft) electionLoop() {
	DPrintf("RaftNode [%d] begin to elect", rf.me)
	for !rf.killed() {
		// 每个轮回睡一秒避免CPU高度空转
		time.Sleep(1 * time.Millisecond)
		rf.startElection()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	now := time.Now()
	elapses := now.Sub(rf.lastActiveTime)
	// 超时的话会从Follower转为Candidate
	if rf.role == Follower {
		if elapses >= rf.electionTimeOut {
			rf.role = Candidate
			DPrintf("RaftNode [%d] changed from follower to candidate at term %d", rf.me, rf.currentTerm)
		}
	}
	if rf.role == Candidate && elapses >= rf.electionTimeOut {
		rf.lastActiveTime = now
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.persist()
		// 准备开始发起投票，构造投票流程
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs),
			// go不支持三元表达式，所以term赋值写在最后
		}
		lastLogTerm := 0
		if len(rf.logs) != 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		args.LastLogTerm = lastLogTerm
		// 参数赋予流程初步结束，为了避免RPC请求停顿影响效率，先放掉锁
		rf.mu.Unlock()
		// 开始for循环不断发起响应通知进行处理
		cond := sync.NewCond(&rf.mu)
		// 初始化时全部peer里自己就已经给自己投了一票，因此初始化为1
		finished := 1
		count := 1
		maxTerm := rf.currentTerm
		currentTerm := rf.currentTerm
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			// 发送请求并处理返回结果
			go func(server int, args *RequestVoteArgs) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, &reply)
				rf.mu.Lock()
				finished += 1
				DPrintf("RaftNode [%d] send Rpc to RaftNode [%d] and get response,%v,now finish %d", rf.me, server, ok, finished)
				if ok {
					// 进行逻辑运行，除了降级逻辑外，还是尽量将逻辑推迟到下面
					if reply.VoteGranted == true {
						count += 1
					} else {
						if reply.Term > maxTerm {
							// 状态进行改变清空
							maxTerm = reply.Term
						}
					}
					DPrintf("RaftNode [%d] get result %d,vote %d", rf.me, finished, count)
				}
				cond.Broadcast()
				rf.mu.Unlock()
			}(index, &args)
		}
		rf.mu.Lock()
		// 想要直接往后走：1、投票完毕 2、得到超过半数票数无需继续等待 3、已经不再是candidate状态
		// 注意小于等于与大于等于的使用，一开始在这里卡死便是因为小于等于直接满足
		for count <= len(rf.peers)/2 && finished != len(rf.peers) && rf.role == Candidate {
			DPrintf("finish %d,count %d", finished, count)
			cond.Wait()
		}
		// 检查是否是当前的term
		// 不是当前term直接结束
		if currentTerm != rf.currentTerm {
			return
		}
		// 检查当前最大任期是否比现在任期大
		if maxTerm > currentTerm {
			rf.currentTerm = maxTerm
			rf.role = Follower
			rf.votedFor = -1
			rf.leaderId = -1
			rf.persist()
			return
		}
		// 不是candidate的话直接结束
		if rf.role != Candidate {
			return
		}
		DPrintf("boolean %v,count %d,total %d", count > len(rf.peers)/2, count, len(rf.peers))
		// 否则对状态进行判断
		if count > len(rf.peers)/2 {
			rf.role = Leader
			rf.leaderId = rf.me
			rf.nextIndex = make([]int, len(rf.peers))
			// 被选举为leader后重新初始化nextIndex和matchIndex
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.logs) + 1
			}
			rf.matchIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.matchIndex[i] = 0
			}
			rf.lastBroadcastTime = time.Unix(0, 0) // 设置为原始时间，保证立刻发送心跳
			rf.persist()
			DPrintf("RaftNode  [%d] become a leader", rf.me)
		}
	}
	return
}

func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)
		rf.startAppend()
	}
}

func (rf *Raft) startAppend() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有leader才能发送心跳
	if rf.role != Leader {
		return
	}
	now := time.Now()
	if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
		return
	}
	rf.lastBroadcastTime = now
	// for循环后没有其他等待的语句，故这里不必添加unlock语句
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[index] - 1, // 检查是否之前的记录能对上
		}
		if args.PrevLogIndex > len(rf.logs) {
			fmt.Printf("leader:%d\n", rf.me)
			fmt.Printf("args.prevLogIndex:%+v,len rf.logs %d\n", rf.nextIndex, len(rf.logs))
		}
		if args.PrevLogIndex > 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term // 检查是否之前的记录能对上，保证了一致性
		}
		// 初始为[0:0]，符合语法格式
		args.Entries = append(args.Entries, rf.logs[rf.nextIndex[index]-1:]...) // 如果能匹配上就直接将之后的记录加上去
		go func(server int, args *AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if rf.currentTerm != args.Term {
					return
				}
				if reply.Term > rf.currentTerm {
					if rf.role == Leader {
						DPrintf("RaftNode [%d] changed from leader to follower", rf.me)
					}
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.leaderId = -1
					rf.votedFor = -1
					rf.persist()
					return
				}
				if reply.Success {
					// 同步逻辑成功
					// 更新matchIndex和nextIndex，并尝试更新commitIndex
					k := rf.nextIndex[server]
					// 这里需要按照args的信息进行更新而不是直接更新，以防止因为网络延迟出现重复添加的问题
					// rf.nextIndex[server] += len(args.Entries)
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					// used to debug with go test -run TestFigure8Unreliable2C
					if rf.nextIndex[server] > len(rf.logs)+1 {
						fmt.Printf("nextIndex now:%d\n", k)
						fmt.Printf("leader:%d server:%d \n", rf.me, server)
						fmt.Printf("args info\n")
						fmt.Printf("Term %d, LeaderId:%d PrevLogIndex:%d PrevLogTerm:%d Entries Length:%d LeaderCommit:%d\n", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
						fmt.Printf("reply:%+v\n", reply)
						fmt.Printf("4 leader:%d server:%d next index %d is bigger than rf.logs %d\n", rf.me, server, rf.nextIndex[server], len(rf.logs))
					}
					DPrintf("RaftNode[%d] send append request to server：%d with nextIndex %d\n", rf.me, server, rf.nextIndex[server])
					// 排序寻找中位数，这里有奇数个peer，故大于等于中位数的必定会占一半以上
					sortedMatchIndex := make([]int, 0)
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							// 同一个周期内自身必定最大，可以忽略
							continue
						}
						sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
					}
					sort.Ints(sortedMatchIndex)
					newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
					DPrintf("AppendEntries back to leader RaftNode[%d] newCommitIndex %d LogLength %d\n", rf.me, newCommitIndex, len(rf.logs))
					DPrintf("AppendEntries back and leader's NextIndex is %+v\n", rf.nextIndex)
					DPrintf("AppendEntries and leader's MatchIndex is %+v\n", rf.matchIndex)
					if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex-1].Term == rf.currentTerm {
						rf.commitIndex = newCommitIndex
					}
				} else {
					// 同步失败，需要减小返回值
					// rf.nextIndex[server] -= 1
					// 效率优化版
					if reply.ConflictTerm == -1 {
						// 没有对应的prevIndex
						rf.nextIndex[server] = reply.ConflictIndex + 1
						if reply.ConflictIndex+1 > len(rf.logs) {
							fmt.Printf("1 next index %d is bigger than rf.logs %d\n", reply.ConflictIndex+1, len(rf.logs))
						}
					} else {
						// 有对应的prevIndex，但是term对不上
						lastIndex := -1
						for i := args.PrevLogIndex; i >= 1; i-- {
							if rf.logs[i-1].Term == reply.ConflictTerm {
								lastIndex = i
								break
							}
						}
						if lastIndex != -1 {
							rf.nextIndex[server] = lastIndex + 1
							if lastIndex+1 > len(rf.logs) {
								fmt.Printf("2 next index %d is bigger than rf.logs %d\n", lastIndex+1, len(rf.logs))
							}
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
							if reply.ConflictIndex > len(rf.logs) {
								fmt.Printf("3 next index %d is bigger than rf.logs %d\n", reply.ConflictIndex, len(rf.logs))
							}
						}
					}
					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}
				}
			}
		}(index, &args)
	}
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		var appliedMsg = make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedMsg = append(appliedMsg, ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
				})
			}
		}()
		// 释放掉锁后批量读入channel以防锁被卡住无法释放
		for _, msg := range appliedMsg {
			applyCh <- msg
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是leader直接返回
	if rf.role != Leader {
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
	index = len(rf.logs) // 保证index从1开始
	term = rf.currentTerm
	isLeader = true
	rf.persist()
	DPrintf("RaftNode [%d] add command %d in term %d, index %d", rf.me, command, term, index)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = Follower
	rf.applyCh = applyCh
	rf.lastActiveTime = time.Now()
	// 设置时间间隔为300-500
	rf.electionTimeOut = time.Millisecond * time.Duration(200+rand.Intn(200))
	rf.mu = sync.Mutex{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.electionLoop()
	go rf.appendEntriesLoop()
	go rf.applyLogLoop(applyCh)
	// 需要启动选举流程和leader的append逻辑
	DPrintf("RaftNode [%d] startup", rf.me)
	return rf
}
