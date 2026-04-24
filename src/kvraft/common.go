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
	ClientId int64 // 标记该请求是谁发的
	SeqNum int64 // 标记消息的全局序号 用于请求重试 满足幂等性
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64
	SeqNum int64
}

type GetReply struct {
	Err   Err
	Value string
}
