package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"       // 针对get请求进行处理，表示没有对应的key
	ErrWrongLeader = "ErrWrongLeader" // 找到错误的leader
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
	ClientId int64 // 记录发送请求的client
	SeqId    int64 // 记录该client发送的id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int64
}

type GetReply struct {
	Err   Err
	Value string
}
