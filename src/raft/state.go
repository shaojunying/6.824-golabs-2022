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
	log.Printf("[server:%d, role: %s, term: %d] %s\n", raft.me, role, raft.persister.currentTerm, fmt.Sprintf(format, args...))
}

func (f FollowerState) GenerateTimeout() time.Duration {
	return GenerateElectionTimeout()
}

func (f FollowerState) HandlerAppendEntries(request *AppendEntriesArgs) *AppendEntriesReply {
	rf := f.raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Log(rf, "收到一个来自%d心跳包", request.LeaderId)
	reply := AppendEntriesReply{
		Term: rf.persister.currentTerm,
	}
	if request.Term < rf.persister.currentTerm {
		Log(rf, "拒绝了收到的心跳包")
		reply.Success = false
	} else {
		rf.updateCurrentTerm(request.Term)
		reply.Success = true
		Log(rf, "接受了收到的心跳包")
		rf.receivedAppendEntries = true
		//rf.timer.Reset(GenerateElectionTimeout())
	}
	return &reply
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.persister.currentTerm
	reply := RequestVoteReply{
		Term: currentTerm,
	}
	if request.Term < currentTerm {
		reply.VoteGranted = false
	} else {
		rf.updateCurrentTerm(request.Term)
		if rf.persister.voteFor == nil || *rf.persister.voteFor == request.CandidateId {
			// TODO 处理up to date的情况
			rf.receivedAppendEntries = true
			rf.voteFor(request.CandidateId)
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	Log(rf, "是否赞成%d: %t", request.CandidateId, reply.VoteGranted)
	return &reply
}

type CandidateState struct {
	raft *Raft
}

func (c CandidateState) GenerateTimeout() time.Duration {
	return GenerateElectionTimeout()
}

func (c CandidateState) HandlerAppendEntries(request *AppendEntriesArgs) *AppendEntriesReply {
	raft := c.raft
	raft.mu.Lock()
	defer raft.mu.Unlock()
	reply := AppendEntriesReply{
		Term: raft.persister.currentTerm,
	}
	Log(raft, "收到一个心跳包")
	if request.Term >= raft.persister.currentTerm {
		raft.updateCurrentTerm(request.Term)
		Log(raft, "收到的心跳包被接受")
		reply.Success = true
		raft.currState = raft.Follower
		raft.receivedAppendEntries = true
		//raft.timer.Reset(GenerateElectionTimeout())
	} else {
		Log(raft, "收到的心跳包被拒绝")
		reply.Success = false
	}
	return &reply
}

func (c CandidateState) HandlerTimeOut() {
	raft := c.raft
	raft.mu.Lock()
	defer raft.mu.Unlock()
	Log(raft, "CandidateState HandlerTimeOut")

	raft.transitBeCandidate()
}

func (c CandidateState) HandlerRequestVote(request *RequestVoteArgs) *RequestVoteReply {
	raft := c.raft
	raft.mu.Lock()
	defer raft.mu.Unlock()
	reply := RequestVoteReply{
		Term: c.raft.persister.currentTerm,
	}
	if request.Term > raft.persister.currentTerm {
		raft.updateCurrentTerm(request.Term)
		raft.voteFor(request.CandidateId)
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	Log(raft, "是否赞成%d: %t", request.CandidateId, reply.VoteGranted)

	return &reply
}

type LeaderState struct {
	raft *Raft
}

func (l LeaderState) GenerateTimeout() time.Duration {
	return GenerateHeartBeatTimeOut()
}

func (l LeaderState) HandlerAppendEntries(request *AppendEntriesArgs) *AppendEntriesReply {
	rf := l.raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Log(rf, "收到一个心跳包")
	reply := AppendEntriesReply{
		Term: rf.persister.currentTerm,
	}
	if request.Term > rf.persister.currentTerm {
		rf.updateCurrentTerm(request.Term)
		rf.transitToFollower()
		rf.receivedAppendEntries = true
		//rf.timer.Reset(GenerateHeartBeatTimeOut())
		reply.Success = true
	} else {
		reply.Success = false
	}
	return &reply
}

func (l LeaderState) HandlerTimeOut() {
	rf := l.raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Log(rf, "超时")
	rf.receivedAppendEntries = true
	//rf.timer.Reset(GenerateHeartBeatTimeOut())
	rf.sendHeartBeatToAll()
}

func (l LeaderState) HandlerRequestVote(request *RequestVoteArgs) *RequestVoteReply {
	rf := l.raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	response := RequestVoteReply{
		Term: rf.persister.currentTerm,
	}
	if request.Term > rf.persister.currentTerm {
		// 更新Term，并投票
		rf.updateCurrentTerm(request.Term)
		rf.voteFor(request.CandidateId)
		// 转为Follower
		response.VoteGranted = true
	} else {
		response.VoteGranted = false
	}
	Log(rf, "是否赞成%d: %t", request.CandidateId, response.VoteGranted)
	return &response
}
