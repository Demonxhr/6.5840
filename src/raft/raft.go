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
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//type Log struct {
//	Entries       []Entry
//	FirstLogIndex int
//	LastLogIndex  int
//}

//func (log *Log) appendL(newEntries ...Entry) {
//	log.Entries = append(log.Entries[:log.LastLogIndex-log.FirstLogIndex+1], newEntries...)
//	log.LastLogIndex += len(newEntries)
//
//}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state   锁
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]       peer 数组的下标  保存每个每个服务器
	dead      int32               // set by Kill()

	// my code  3a
	currentTerm    int         // 当前任期
	voteFor        int         //把票投给了谁
	state          int         // 当前状态  跟随者、候选者、领导者
	electionTimer  *time.Timer // 选举计时器
	heartbeatTimer *time.Timer //心跳计时器

	logs        []LogEntry
	commitIndex int //已知提交的最高日志项索引
	lastApplied int //应用于状态机最高日志项的索引  ？？？？？？？？？？？？？？？

	//由leader维护   nextIndex初始化为 log index + 1  matchIndex初始化为0
	nextIndex  []int //对于每个服务器，要发送到该服务器的下一个日志条目的索引
	matchIndex []int //对于每个服务器，已知要在服务器上复制的最高日志条目的索引

	ApplyCh chan ApplyMsg

	notifyApplyCh chan struct{} // 日志应用通道
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func Printnowtime() {
	// 获取当前本地时间
	currentTime := time.Now()

	// 使用默认格式输出当前时间
	fmt.Println("当前时间:", currentTime)

	// 自定义格式输出当前时间
	const layout = "2006-01-02 15:04:05" // Go的诞生时间，用于定义时间格式
	formattedTime := currentTime.Format(layout)
	fmt.Println("当前时间:", formattedTime)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func Getrandontime() time.Duration {
	rand.Seed(time.Now().UnixNano()) // 初始化随机种子
	return time.Duration(rand.Intn(150) + 450)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// （从跟随者转变为候选人   增加当前任期+1  ，并给自己投票， 然后给向所有服务器发出RequestVoteArgs拉票）
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人的任期
	CandiadateId int //  应该是当前id  即当前peer's index  告诉要投票的id
	LastLogIndex int //  候选人最后的日志下标
	LastLogTerm  int // 候选人最后日志任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 收到要投票服务器的任期
	VoteGranted bool //  是否投票
}

// example RequestVote RPC handler.  如果当前任期比收到的要大  拒绝投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// term < currentTerm  返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 接受term大于当前term
	if args.Term > rf.currentTerm {
		//将当前term置为获得的term
		rf.currentTerm = args.Term
		//不是最新的vote  将votefor置-1,到下面再赋值
		rf.voteFor = -1
		rf.state = FOLLOWER
	}

	// 不为-1且不是给该候选人投过票的情况,则拒绝给他投票
	if rf.voteFor != -1 && rf.voteFor != args.CandiadateId {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 候选人日志最新才给候选人投票
	if len(rf.logs) > 0 {
		rfLastLogIndex := len(rf.logs) - 1
		rfLastLogTerm := rf.logs[rfLastLogIndex].Term
		// 当当前rf的最后一条日志任期大于候选人日志最后一条日志任期 或  任期相同时 最后一条索引大于候选人索引  时  拒绝投票
		// 解决了follower在断联后任期不断增加  但没有收到最新日志的情况
		if rfLastLogTerm > args.LastLogTerm || rfLastLogTerm == args.LastLogTerm && rfLastLogIndex > args.LastLogIndex {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	}

	// 给他投票    会不会有一个给同一个候选人多次投票的情况？？？  应该不会
	// 回复在中途丢失了   但这边已经投过票了  但候选人没收到的情况  再投一次该候选人
	// 接受到心跳包选举时间重置
	rf.electionTimer.Reset(Getrandontime() * time.Millisecond)

	rf.voteFor = args.CandiadateId

	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// 用于复制日志条目 ， 也作为心跳
type AppendEntriesArgs struct {
	Term     int
	LeaderId int //.......

	PrevLogIndex int //新条目前的index
	PrevLogTerm  int //PrevLogIndex的任期

	Entries      []LogEntry //要存储的日志
	LeaderCommit int        //领导者的commitindex
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // 记录冲突的下标   不用每次都-1 减少rpc的次数
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	// 加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果当前任期大于接受到的任期  返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果接受的任期大于当前任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = FOLLOWER
	}

	// 接受到心跳包选举时间重置
	a := Getrandontime()
	rf.electionTimer.Reset(a * time.Millisecond)
	//Printnowtime()

	reply.Term = rf.currentTerm

	//如果这个要接受的日志index大于本服务器nextIndex
	// 如果这个日志之前的日志任期不一样
	if len(rf.logs)-1 < args.PrevLogIndex || (args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		// 没有冲突  但log缺失很多
		if args.PrevLogIndex > len(rf.logs)-1 {
			reply.ConflictTerm = -1
			reply.ConflictIndex = len(rf.logs)
		} else {
			// 冲突
			reply.ConflictTerm = rf.logs[args.PrevLogTerm].Term
			index := args.PrevLogIndex - 1
			for index > 0 && rf.logs[index].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}
		return
	}

	//!!!!!!!!!有问题     Entries和log重合的部分不添加
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > len(rf.logs)-1 || rf.logs[index].Term != entry.Term {
			rf.logs = rf.logs[:index]
			// 深拷贝
			rf.logs = append(rf.logs, append([]LogEntry{}, args.Entries[i:]...)...)

			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs) {

			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}

		// 应用
		go func() { rf.notifyApplyCh <- struct{}{} }()
	}

	reply.Success = true

}

// example code to send a RequestVote RPC to a server.  向服务器发送RequestVote RPC的示例代码。
// server is the index of the target server in rf.peers[].  server是rf.peers[]中目标服务器的索引。
// expects RPC arguments in args.                           参数中应包含RPC参数
// fills in *reply with RPC reply, so caller should           用RPC回复填充*reply，所以调用者应该
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadCastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderCommit: rf.me}
		// !!!!!!!!!!!!!!!!!!!这边问题？？？？  没问题了    每个peer对应不同的 nextIndex
		args.PrevLogIndex = rf.nextIndex[i] - 1

		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		// 使用深拷贝   不然会有问题 ！！！！！   浅拷贝只是引用   指向同一块内存区域  当外部改变时 同时改变
		entries := rf.logs[rf.nextIndex[i]:]

		args.Entries = make([]LogEntry, len(entries))
		copy(args.Entries, entries)

		reply := AppendEntriesReply{}
		// 对每个peer起一个协程 用于复制日志
		go func(peer int) {
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {

				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.state = FOLLOWER
				rf.electionTimer.Reset(Getrandontime() * time.Millisecond)
				return
			}
			if reply.Success {
				// 重复的回应  忽略  已经添加过了
				if rf.nextIndex[peer]+len(args.Entries) > len(rf.logs) {
					return
				}
				// 设置nextIndex 为 加上新日志后下一格
				rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1

				N := len(rf.logs) - 1
				for N > rf.commitIndex {
					if rf.logs[N].Term != rf.currentTerm {
						N--
						continue
					}
					cnt := 1
					for _, matchidx := range rf.matchIndex {
						if matchidx >= N {
							cnt++
						}
					}
					if cnt <= len(rf.peers)/2 {
						N--
						continue
					}
					rf.commitIndex = N
					go func() { rf.notifyApplyCh <- struct{}{} }()
					return
				}
			} else {
				// 返回失败
				index := -1
				found := false
				for i, entry := range rf.logs {
					if entry.Term == reply.ConflictTerm {
						index = i
						found = true
					} else if found {
						break
					}
				}
				if found {
					rf.nextIndex[peer] = index + 1
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex

				}

			}
		}(i)

	}
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

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有leader才会发送添加日志
	if rf.state != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	//如果是leader写日志 复制日志
	// 将command保存在Entry中
	// 将Entry保存到日志中
	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, entry)
	index = len(rf.logs) - 1
	term = rf.currentTerm
	go rf.broadCastAppendEntries()
	return index, term, isLeader
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
func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	state := rf.state == LEADER
	rf.mu.Unlock()
	return state
}

// 心跳  即发送添加日志rpc
func (rf *Raft) heartbeat() {
	wakeChPool := make([]chan struct{}, len(rf.peers))
	doneChPool := make([]chan struct{}, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wakeChPool[i] = make(chan struct{})
		doneChPool[i] = make(chan struct{})
		go func(i int) {
			for {
				select {
				case <-wakeChPool[i]:
					rf.mu.Lock()
					args := AppendEntriesArgs{LeaderId: rf.me, LeaderCommit: rf.commitIndex}
					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
					entries := rf.logs[rf.nextIndex[i]:]
					args.Entries = make([]LogEntry, len(entries))
					copy(args.Entries, entries)
					args.Term = rf.currentTerm
					rf.mu.Unlock()
					reply := AppendEntriesReply{}

					go func(peer int) {

						if ok := rf.sendAppendEntries(i, &args, &reply); !ok {

							return
						}

						rf.mu.Lock()
						//返回的任期比当前的大
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.voteFor = -1
							rf.state = FOLLOWER
							rf.mu.Unlock()

							return
						}
						rf.mu.Unlock()
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if !reply.Success {
							// 返回失败
							index := -1
							found := false
							for i, entry := range rf.logs {
								if entry.Term == reply.ConflictTerm {
									index = i
									found = true
								} else if found {
									break
								}
							}
							if found {
								rf.nextIndex[peer] = index + 1
							} else {
								rf.nextIndex[peer] = reply.ConflictIndex
							}
						}
					}(i)
				case <-doneChPool[i]:
					return
				}
			}
		}(i)
	}

	broadcast := func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				wakeChPool[i] <- struct{}{}
			}(i)
		}
	}
	broadcast()

	rf.heartbeatTimer.Reset(50 * time.Millisecond)
	for {
		<-rf.heartbeatTimer.C
		if rf.killed() || !rf.isLeader() {
			break
		}

		broadcast()
		rf.heartbeatTimer.Reset(50 * time.Millisecond)
		//Printnowtime()
	}

	//停掉协程
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			doneChPool[i] <- struct{}{}
		}(i)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 超时进入选举  将自己当前任期加一
	rf.currentTerm++

	//Printnowtime()
	// 把票投给自己
	rf.voteFor = rf.me
	// 重置 选举时间  防止未选举出
	rf.electionTimer.Reset(Getrandontime() * time.Millisecond)
	rf.mu.Unlock()

	// 设置拉票请求
	args := RequestVoteArgs{CandiadateId: rf.me, LastLogIndex: len(rf.logs) - 1, LastLogTerm: rf.logs[len(rf.logs)-1].Term}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	rf.mu.Unlock()

	// 保存投票的数 放在通道中
	voteCh := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// 起一个goroutine 用于发送rpc投票请求
		go func(i int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &args, &reply); !ok {
				voteCh <- false
				return
			}
			rf.mu.Lock()
			// 返回任期大于当前任期
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.state = FOLLOWER
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(i)
	}

	voteCnt := 1
	voteGrantedCnt := 1
	for voteGranted := range voteCh {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != CANDIDATE {
			break
		}
		if voteGranted {
			voteGrantedCnt++
		}
		//大于一半的  当选leader
		if voteGrantedCnt > len(rf.peers)/2 {
			rf.mu.Lock()
			rf.state = LEADER
			rf.mu.Unlock()

			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}

			//go rf.broadCastAppendEntries()
			// 发送心跳 协程
			go rf.heartbeat()

			break
		}

		voteCnt++
		// 记录总共有多少投了票   如果大于总数则退出该协程  防止资源浪费
		if voteCnt == len(rf.peers) {
			break
		}
	}

}

// 当超时 进行选举
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			// 如果是leader超时了  没事   不会给自己发心跳包
			if rf.state == LEADER {
				rf.mu.Unlock()
				break
			}
			rf.state = CANDIDATE
			rf.mu.Unlock()

			go rf.startElection()
		case <-rf.notifyApplyCh:
			rf.mu.Lock()
			lastApplied := rf.lastApplied
			rf.lastApplied = rf.commitIndex

			// ... 语法糖 将切片展开为一个参数列表
			entries := append([]LogEntry{}, rf.logs[lastApplied+1:rf.commitIndex+1]...)
			rf.mu.Unlock()

			for i, entry := range entries {
				command := entry.Command

				rf.ApplyCh <- ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: lastApplied + i + 1,
				}
			}
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
	rf.dead = 0

	rf.voteFor = -1
	rf.state = FOLLOWER
	rf.currentTerm = 0

	rf.electionTimer = time.NewTimer(Getrandontime() * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(50 * time.Millisecond)

	rf.ApplyCh = applyCh
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm})

	rf.notifyApplyCh = make(chan struct{})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
