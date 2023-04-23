package raft

import (
	"fmt"
	"log"
	"time"
)

type State interface {
	HandlerTimeOut()
	HandlerRequestVote(request *RequestVoteArgs) *RequestVoteReply
	HandlerAppendEntries(request *AppendEntriesArgs) *AppendEntriesReply
	GenerateTimeout() time.Duration
}
type FollowerState struct {
	raft *Raft
}

// Log 调用该方法时应该持有相应Raft锁
func Log(raft *Raft, format string, args ...interface{}) {
	role := "Follower"
	if raft.currState == raft.Candidate {
		role = "Candidate"
	}
	if raft.currState == raft.Leader {
		role = "Leader"
	}
	log.Printf("[server:%d, role: %s, Term: %d] %s\n", raft.me, role, raft.persister.currentTerm, fmt.Sprintf(format, args...))
}

func (f FollowerState) GenerateTimeout() time.Duration {
	return GenerateElectionTimeout()
}

func (f FollowerState) HandlerAppendEntries(request *AppendEntriesArgs) *AppendEntriesReply {
	rf := f.raft
	return rf.handlerAppendEntries(request)
}

// Leader使用该函数处理已发出心跳包的response
func handlerHeartBeatResponse(rf *Raft, reply RequestVoteReply) {
	rf.updateCurrentTerm(reply.Term)
}

// HandlerTimeOut 定时器超时，Follower升为Candidate，开始选举
func (f FollowerState) HandlerTimeOut() {
	rf := f.raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Log(rf, "time out, and will be candidate")
	rf.transitBeCandidate()
}

func (f FollowerState) HandlerRequestVote(request *RequestVoteArgs) *RequestVoteReply {
	rf := f.raft
	return rf.handlerRequestVote(request)
}

type CandidateState struct {
	raft *Raft
}

func (c CandidateState) GenerateTimeout() time.Duration {
	return GenerateElectionTimeout()
}

func (c CandidateState) HandlerAppendEntries(request *AppendEntriesArgs) *AppendEntriesReply {
	rf := c.raft
	return rf.handlerAppendEntries(request)
}

func (c CandidateState) HandlerTimeOut() {
	raft := c.raft
	raft.mu.Lock()
	defer raft.mu.Unlock()
	Log(raft, "CandidateState HandlerTimeOut")

	raft.transitBeCandidate()
}

func (c CandidateState) HandlerRequestVote(request *RequestVoteArgs) *RequestVoteReply {
	rf := c.raft
	return rf.handlerRequestVote(request)
}

type LeaderState struct {
	raft *Raft
}

func (l LeaderState) GenerateTimeout() time.Duration {
	return GenerateHeartBeatTimeOut()
}

func (l LeaderState) HandlerAppendEntries(request *AppendEntriesArgs) *AppendEntriesReply {
	rf := l.raft
	return rf.handlerAppendEntries(request)
}

func (l LeaderState) HandlerTimeOut() {
	rf := l.raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Log(rf, "Leader定时器超时")
	rf.receivedAppendEntries = true
	//rf.timer.Reset(GenerateHeartBeatTimeOut())
	rf.sendHeartBeatToAll()
}

func (l LeaderState) HandlerRequestVote(request *RequestVoteArgs) *RequestVoteReply {
	rf := l.raft
	return rf.handlerRequestVote(request)
}
