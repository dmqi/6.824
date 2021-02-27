package kvraft

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string // "put/append"
	Key      string
	Value    string
	ClientId int64
	SeqId    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string]string
	chMap map[int]chan Op
	cl2seq map[int64]int
	persister *raft.Persister
}

func (kv *KVServer) put(idx int, createIfNotExist bool) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		if !createIfNotExist { return nil }
		kv.chMap[idx] = make(chan Op, 1)
	}
	return kv.chMap[idx]
}

func equalOp(a Op, b Op) bool {
	return a.Key == b.Key && a.Value == b.Value && a.Type == b.Type && a.SeqId == b.SeqId && a.ClientId == b.ClientId
}

func (kv *KVServer) beNotified(ch chan Op, index int) Op {
	select {
	case op :=<- ch :
		close(ch)
		kv.mu.Lock()
		delete(kv.chMap, index)
		kv.mu.Unlock()
		return op
	case <- time.After(time.Duration(600)*time.Millisecond):
		return Op{}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here
	reply.Err = ErrWrongLeader
	originOp := Op{"Get", args.Key, "", 0, 0}
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader { return }
	ch := kv.put(index, true)
	op := kv.beNotified(ch, index)
	if equalOp(op, originOp) {
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = ErrWrongLeader
	originOp := Op{ args.Op, args.Key, args.Value, args.ClientId, args.SeqId }
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader { return }

	ch := kv.put(index, true)
	op := kv.beNotified(ch, index)

	if equalOp(op, originOp) {
		reply.Err = OK
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

func (kv *KVServer) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cl2seq map[int64]int

	if d.Decode(&db) != nil || d.Decode(&cl2seq) != nil {
		//log.Fatal("readSnapShot ERROR for KVServer:%v", kv.me)
	} else {
		kv.mu.Lock()
		kv.db, kv.cl2seq = db, cl2seq
		kv.mu.Unlock()
	}
}

func (kv *KVServer) needSnapShot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	threshold := 10
	return kv.maxraftstate > 0 &&
		kv.maxraftstate - kv.persister.RaftStateSize() < kv.maxraftstate / threshold
}

func (kv *KVServer) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.cl2seq)
	kv.mu.Unlock()
	//log.Println("before compact log size ", kv.persister.RaftStateSize())
	kv.rf.DoSnapShot(index, w.Bytes())
	//log.Println("after compact log size ", kv.persister.RaftStateSize())
}

func send(notifyCh chan Op, op Op) {
	select {
	case <- notifyCh:
	default:
	}
	notifyCh <- op
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.cl2seq = make(map[int64]int)
	kv.readSnapShot(kv.persister.ReadSnapshot()) // 恢复db和cl2seq
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			if kv.killed() { return }
			applyMsg := <- kv.applyCh
			if !applyMsg.CommandValid {
				kv.readSnapShot(applyMsg.SnapShot)
				continue
			}
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			lastSeq, found := kv.cl2seq[op.ClientId]
			if !found || op.SeqId > lastSeq {
				switch op.Type {
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
				kv.cl2seq[op.ClientId] = op.SeqId
			}
			kv.mu.Unlock()
			if kv.needSnapShot() {
				go kv.doSnapShot(applyMsg.CommandIndex) // 重要改动，改完全部pass
			}
			if notifyCh := kv.put(applyMsg.CommandIndex,false); notifyCh != nil {
				send(notifyCh, op)
			}
		}
	}()

	return kv
}
