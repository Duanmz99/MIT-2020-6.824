package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// 对传进来的原始op进行封装
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Index    int    // 写入raft时的index
	Term     int    // 写入raft时的term
	Type     string // 操作类型，put，append，get
	Key      string
	Value    string // Get操作无需管理
	SeqId    int64  // 记录发送号
	ClientId int64  // 记录client号
}

// 记录apply的op对应的结果
type OpContext struct {
	op          *Op
	committed   chan byte // 管道类型无所谓，单纯用来表示是否已完成，完成直接close
	wrongLeader bool      // 表示是否leader发生了变化
	ignored     bool      // 过期请求的RPC应该进行忽略
	keyExist    bool      // 对于get请求进行处理，表示是否
	value       string    // 记录get请求的返回值
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft // 每个rf对应一个raft成员
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDatabase map[string]string  // 储存输入的信息，并提供查询服务
	reqMap     map[int]*OpContext // 储存当前记录的指令信息
	seqMap     map[int64]int64    // 储存执行过的seq

}

func newOpContext(op *Op) (opCtx *OpContext) {
	opCtx = &OpContext{
		op:        op,
		committed: make(chan byte),
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK

	op := &Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 写入raft层
	var isLeader bool
	//fmt.Printf("get %+v\n",op)
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := newOpContext(op)

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kv.reqMap[op.Index] = opCtx
	}()

	// RPC结束前清理上下文
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.Index]; ok {
			// 看是否被覆盖，如果已经被覆盖的话就不应该再进行删除key操作
			// 失败了client就重新发，新的相同请求会对应不同的index
			if one == opCtx {
				delete(kv.reqMap, op.Index)
			}
		}
	}()

	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	select {
	// 指向opCtx的指针，便于随时读取opCtx修改后的值
	case <-opCtx.committed: // 如果提交了
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = ErrWrongLeader
		} else if !opCtx.keyExist { // key不存在
			reply.Err = ErrNoKey
		} else {
			reply.Value = opCtx.value // 返回值
		}
	case <-timer.C: // 如果2秒都没提交成功，让client重试
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK

	op := &Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 写入raft层
	var isLeader bool
	//fmt.Printf("putappend %+v\n",op)
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := newOpContext(op)

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kv.reqMap[op.Index] = opCtx
	}()

	// RPC结束前清理上下文
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.Index]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.Index)
			}
		}
	}()

	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-opCtx.committed: // 如果提交了
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = ErrWrongLeader
		} else if opCtx.ignored {
			// 说明req id过期了，该请求被忽略，对MIT这个lab来说只需要告知客户端OK跳过即可
		}
	case <-timer.C: // 如果2秒都没提交成功，让client重试
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		// 读取已经被apply的msg
		msg := <-kv.applyCh
		cmd := msg.Command
		index := msg.CommandIndex
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			// 将读入的interface{}转为特定类型的数据
			op := cmd.(*Op)
			//fmt.Printf("%+v",op)
			opCtx, existOp := kv.reqMap[index]
			prevSeq, existSeq := kv.seqMap[op.ClientId]
			// 此时处理的可能并不是同一个clientId的指令，而是其他leader复制过来的指令
			kv.seqMap[op.ClientId] = op.SeqId // 保证seq有序上升
			if existOp {
				// 需要检验是否传输过来的op还是在原来的term里，如果term发生变化不一致的话就不执行了
				if opCtx.op.Term != op.Term {
					opCtx.wrongLeader = true
					// 这里是何用意，是否应该直接退出？
					// 尝试退出
					close(opCtx.committed)
					return
				}
			}
			if op.Type == "Put" || op.Type == "Append" {
				if !existSeq || op.SeqId > prevSeq {
					if op.Type == "Put" {
						kv.kvDatabase[op.Key] = op.Value
					} else { // 对应append情况
						if value, exist := kv.kvDatabase[op.Key]; exist {
							kv.kvDatabase[op.Key] = value + op.Value
						} else {
							kv.kvDatabase[op.Key] = op.Value
						}
					}
				} else if existOp {
					// put和append指令不能重复执行
					opCtx.ignored = true
				}
			} else {
				if existOp {
					opCtx.value, opCtx.keyExist = kv.kvDatabase[op.Key]
				}
			}
			DPrintf("RaftNode[%d] applyLoop, kvDatabase[%v]", kv.me, kv.kvDatabase)
			// 唤醒沉睡的RPC
			if existOp {
				close(opCtx.committed)
			}
		}()

	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDatabase = make(map[string]string)
	kv.reqMap = make(map[int]*OpContext)
	kv.seqMap = make(map[int64]int64)

	go kv.applyLoop()
	return kv
}
