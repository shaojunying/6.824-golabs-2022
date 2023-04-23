package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	MinElectionTimeout  = 400
	MaxElectionTimeout  = 600
	MinHeartBeatTimeout = 100
	MaxHeartBeatTimeout = 150
)

func GenerateElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(MaxElectionTimeout-MinElectionTimeout)+MinElectionTimeout) * time.Millisecond
}

func GenerateHeartBeatTimeOut() time.Duration {
	return time.Duration(rand.Intn(MaxHeartBeatTimeout-MinHeartBeatTimeout)+MinHeartBeatTimeout) * time.Millisecond
}

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	timer     *time.Timer

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Follower  State
	Candidate State
	Leader    State

	currState   State
	commitIndex int // 已知的, 已被提交的最大索引
	lastApplied int // 已被应用到State Machine上的最大索引

	// leader
	nextIndex             []int // 要发给Server i的下一个Entry的下标 （初始为leader的last log index + 1）
	matchIndex            []int // Server i已经完成拷贝的最大下标 （初始为0）
	receivedAppendEntries bool
	applyCh               chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.currentTerm, rf.currState == rf.Leader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 候选人的term
	CandidateId int // 候选人的id

	LastLogIndex int // 候选人最后一条日志的index
	LastLogTerm  int // 候选人最后一条日志的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前的term，用于候选人更新自身
	VoteGranted bool // 是否向候选人投票？
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 接收到一个RequestVote请求，处理该请求
	//*reply = *rf.currState.HandlerRequestVote(args)
	res := rf.currState.HandlerRequestVote(args)
	//reply1 := *res
	//log.Printf("看看我是不是0: %v\n", reply1)
	reply.VoteGranted = res.VoteGranted
	reply.Term = res.Term
	//*reply = *res
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	log.Printf("RequestVote RPC from %d to %d : ok: %v request: %v, response: %v\n", rf.me, server, ok, *args, *reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int

	PrevLogIndex int        // 新Entry之前的Entry Index
	PrevLogTerm  int        // 新Entry之前的Entry Term
	Entries      []LogEntry // 应该被存储的entries
	LeaderCommit int        // Leader已提交的最后一个Entry Index
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// 接收到一个RequestVote请求，处理该请求
	res := rf.currState.HandlerAppendEntries(args)
	reply.Success = res.Success
	reply.Term = res.Term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	log.Printf("AppendEntries RPC from %d to %d : ok: %v request: %v, response: %v\n", rf.me, server, ok, *args, *reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 这里Start返回并不代表command已经被提交了，Raft只会不停地尝试写入
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.persister.log)
	term := rf.persister.currentTerm
	isLeader := rf.currState == rf.Leader

	// Your code here (2B).
	if isLeader {
		Log(rf, "是日志")
		// 写入命令
		entry := LogEntry{
			Command: command,
			Term:    rf.persister.currentTerm,
		}
		Log(rf, "写入一条命令command: %v", entry)
		rf.persister.log = append(rf.persister.log, entry)
		Log(rf, "写完之后%v", rf.persister.log[1:])
		rf.sendHeartBeatToAll()
	} else {
		Log(rf, "不是Leader，直接跳过")
	}

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 启动一个定时器，在定时器超时的时候，开始一轮选举
		timeout := rf.currState.GenerateTimeout()
		Log(rf, "准备睡眠%d毫秒", timeout.Milliseconds())
		time.Sleep(timeout)
		//rf.timer = time.NewTimer(GenerateElectionTimeout())
		//<-rf.timer.C
		// 超时
		if rf.receivedAppendEntries {
			rf.receivedAppendEntries = false
		} else {
			rf.currState.HandlerTimeOut()
		}
	}
}

// 向所有人发送心跳包
func (rf *Raft) sendHeartBeatToAll() {
	// 发送心跳包
	Log(rf, "向所有人发送心跳包")
	Log(rf, "log: %v", rf.persister.log)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesTo(i)
	}
}

func (rf *Raft) sendAppendEntriesTo(i int) {
	Log(rf, "向%d发送心跳包", i)
	args := rf.getAppendEntriesArgs(i)
	endLogIndex := len(rf.persister.log)
	reply := AppendEntriesReply{}
	Log(rf, "Before Send Append Entries, %v", args)
	ok := rf.sendAppendEntries(i, &args, &reply)
	if !ok {
		// 向该节点发送网络请求失败, 睡眠之后重试
		log.Printf("Server %d 接收失败，睡眠后重试", i)
		//time.Sleep(10 * time.Millisecond)
		//// 还是Leader，重新发送
		//rf.mu.Lock()
		//defer rf.mu.Unlock()
		//if rf.currState == rf.Leader {
		//	go rf.sendAppendEntriesTo(i)
		//}
		return
	}
	Log(rf, "发送心跳包成功")
	// RPC请求成功
	rf.handlerAppendEntriesResponse(i, reply, endLogIndex)
}

func (rf *Raft) getAppendEntriesArgs(i int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := AppendEntriesArgs{
		Term:         rf.persister.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm:  rf.persister.log[rf.nextIndex[i]-1].Term,
		Entries:      rf.persister.log[rf.nextIndex[i]:],
		LeaderCommit: rf.commitIndex,
	}
	Log(rf, "AppendEntriesArgs.LeaderCommit: %d", args.LeaderCommit)
	return args
}

// 处理心跳包的response
func (rf *Raft) handlerAppendEntriesResponse(receiverId int, reply AppendEntriesReply, endLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.persister.currentTerm {
		// Leader身份应失效
		rf.updateCurrentTerm(reply.Term)
		rf.transitToFollower()
		return
	}
	// TODO 考虑这里是否需要考虑接收到response时已经不是Leader的情况
	if rf.currState != rf.Leader {
		// 已经不是Leader
		return
	}
	if !reply.Success {
		// 递减nextIndex, 重试
		Log(rf, "nextIndex[%d]: %d不匹配，后退后重试", receiverId, rf.nextIndex[receiverId])
		rf.nextIndex[receiverId]--
		go rf.sendAppendEntriesTo(receiverId)
		return
	}
	// RPC请求执行成功
	Log(rf, "Append Entries %d 成功, 最新的nextIndex: %d", receiverId, endLogIndex)
	rf.nextIndex[receiverId] = endLogIndex
	rf.matchIndex[receiverId] = endLogIndex
	for commitIndex := rf.commitIndex + 1; commitIndex <= endLogIndex; commitIndex++ {
		// 自己肯定同意，因此应该从1开始
		count := 1
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			if rf.matchIndex[index] > commitIndex {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			if rf.persister.log[commitIndex].Term == rf.persister.currentTerm {
				rf.commitIndex = commitIndex
				Log(rf, "更新rf.commitIndex为%d", rf.commitIndex)
			}
		} else {
			break
		}
	}
	if rf.commitIndex > rf.lastApplied {
		go rf.saveLogToStateMachine()
	}
}

func (rf *Raft) saveLogToStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i < rf.commitIndex+1; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.persister.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
	}
	if rf.lastApplied < rf.commitIndex {
		Log(rf, "Server i 将 rf.lastApplied 从 %d 更新为 %d", rf.lastApplied, rf.commitIndex)
	}
	rf.lastApplied = rf.commitIndex
}

// 投票给某人
func (rf *Raft) voteFor(candidateId int) {
	// 投票之后就变成follower了, 同时更新当前term的voteFor
	rf.transitToFollower()
	rf.persister.voteFor = &candidateId
}

// 向所有人发送投票包
func (rf *Raft) sendRequestVoteToAllServer() {
	Log(rf, "start to send RequestVote to all Server")
	ch := make(chan RequestVoteReply)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		lastLogIndex := len(rf.persister.log) - 1
		args := RequestVoteArgs{
			Term:         rf.persister.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.persister.log[lastLogIndex].Term,
		}
		reply := RequestVoteReply{}
		go func(i int) {
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				// 发送网络请求失败
				return
			}
			ch <- reply
			handlerHeartBeatResponse(rf, reply)
		}(i)
	}
	go rf.attemptToBeLeader(ch)
}

// 更新currentTerm
func (rf *Raft) updateCurrentTerm(term int) {
	if term > rf.persister.currentTerm {
		rf.persister.currentTerm = term
		rf.persister.voteFor = nil
	}
}

// 状态修改为Leader
func (rf *Raft) transitToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Log(rf, "成为leader")
	rf.currState = rf.Leader

	nextIndex := make([]int, len(rf.peers))
	for i := range nextIndex {
		nextIndex[i] = len(rf.persister.log)
	}
	rf.nextIndex = nextIndex
	matchIndex := make([]int, len(rf.peers))
	for i := range matchIndex {
		matchIndex[i] = 0
	}
	rf.matchIndex = matchIndex

	//rf.timer.Reset(GenerateHeartBeatTimeOut())
	rf.sendHeartBeatToAll()
}

// 状态修改为候选人
func (rf *Raft) transitBeCandidate() {
	rf.persister.currentTerm++
	rf.currState = rf.Candidate
	rf.persister.voteFor = &rf.me
	// 向每个Server发送投票RPC
	rf.sendRequestVoteToAllServer()
}

// 判断自己是否是Leader
func (rf *Raft) attemptToBeLeader(ch chan RequestVoteReply) {
	count := 1 // 自己肯定赞成自己
	for range rf.peers {
		reply := <-ch
		if reply.VoteGranted {
			count++
		}
		//log.Printf("%d received a vote\n", raft.me)
		if count > len(rf.peers)/2 {
			// 变为领导者
			rf.transitToLeader()
			break
		}
	}
}

func (rf *Raft) transitToFollower() {
	rf.currState = rf.Follower
	rf.persister.voteFor = nil
}

func (rf *Raft) handleLogEntry(request *AppendEntriesArgs) bool {
	prevLogIndex := request.PrevLogIndex
	//Log(rf, "prevLogIndex: %d, log: %v", prevLogIndex, rf.persister.log[1:])
	Log(rf, "prevLogIndex: %d", prevLogIndex)
	if prevLogIndex >= len(rf.persister.log) || rf.persister.log[prevLogIndex].Term != request.PrevLogTerm {
		Log(rf, "handleLogEntry 失败")
		return false
	} else {
		rf.persister.log = rf.persister.log[:prevLogIndex+1]
		rf.persister.log = append(rf.persister.log, request.Entries...)
		Log(rf, "handleLogEntry 成功, log: %v", rf.persister.log)
		return true
	}
}

func (rf *Raft) shouldVote(request *RequestVoteArgs) bool {
	// 比较最后一个Log的term，
	// - self > candidate: false
	// - self < candidate: true
	// - self == candidate:
	// 	比较日志长度
	// 	- self > candidate: false
	// 	- self < candidate: true
	// 	- self == candidate: true
	//	// 	- 相等的时候仍然可以投票
	lastLogIndex := len(rf.persister.log) - 1
	lastLogTerm := rf.persister.log[lastLogIndex].Term
	if lastLogTerm != request.LastLogTerm {
		return lastLogTerm < request.LastLogTerm
	}
	return lastLogIndex <= request.LastLogIndex
}

func (rf *Raft) isValidAppendEntries(request *AppendEntriesArgs) bool {
	if request.Term < rf.persister.currentTerm {
		return false
	}
	if request.Term > rf.persister.currentTerm {
		// 选举出了新的Leader
		rf.transitToFollower()
	}
	prevLogIndex := request.PrevLogIndex
	if prevLogIndex >= len(rf.persister.log) || rf.persister.log[prevLogIndex].Term != request.PrevLogTerm {
		return false
	}
	return true
}

func (rf *Raft) handlerAppendEntries(request *AppendEntriesArgs) *AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Log(rf, "收到一个Append Entries包")
	rf.updateCurrentTerm(request.Term)
	reply := AppendEntriesReply{
		Term: rf.persister.currentTerm,
	}
	if !rf.isValidAppendEntries(request) {
		reply.Success = false
		return &reply
	}

	prevLogIndex := request.PrevLogIndex
	rf.persister.log = rf.persister.log[:prevLogIndex+1]
	rf.persister.log = append(rf.persister.log, request.Entries...)
	Log(rf, "handleLogEntry 成功, log: %v", rf.persister.log)
	Log(rf, "接受了收到的心跳包")

	rf.commitIndex = request.LeaderCommit
	go rf.saveLogToStateMachine()

	rf.transitToFollower()

	reply.Success = true

	rf.receivedAppendEntries = true
	//rf.timer.Reset(GenerateElectionTimeout())

	return &reply
}

func (rf *Raft) handlerRequestVote(request *RequestVoteArgs) *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateCurrentTerm(request.Term)
	currentTerm := rf.persister.currentTerm
	reply := RequestVoteReply{
		Term: currentTerm,
	}
	if request.Term < currentTerm {
		reply.VoteGranted = false
		return &reply
	}
	if rf.persister.voteFor != nil && *rf.persister.voteFor != request.CandidateId {
		reply.VoteGranted = false
		return &reply
	}

	if !rf.shouldVote(request) {
		reply.VoteGranted = false
		return &reply
	}

	rf.transitToFollower()

	rf.receivedAppendEntries = true
	rf.voteFor(request.CandidateId)
	reply.VoteGranted = true

	Log(rf, "是否赞成%d: %t", request.CandidateId, reply.VoteGranted)
	return &reply
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
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 应该在这里启动启动Raft服务
	rf.Follower = FollowerState{raft: rf}
	rf.Candidate = CandidateState{raft: rf}
	rf.Leader = LeaderState{raft: rf}
	rf.currState = rf.Follower
	rf.applyCh = applyCh

	rf.persister.log = []LogEntry{{
		Command: nil,
		Term:    -1,
	}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
