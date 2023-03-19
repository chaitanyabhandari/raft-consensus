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
	"math"
	"math/rand"
	"raft/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Candidate State = iota
	Follower
	Leader
)

func (s State) String() string {
	states := [...]string{"Candidate", "Follower", "Leader"}
	if s < Candidate || s > Leader {
		return "Unknown"
	}
	return states[s]
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	LogEntries   []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	CommitIndex  int
}

type AppendEntriesReply struct {
	Appended bool
	Term     int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                   sync.Mutex          // Lock to protect shared access to this peer's state
	peers                []*labrpc.ClientEnd // RPC end points of all peers
	me                   int                 // this peer's index into peers[]
	dead                 int32               // set by Kill()
	currentTerm          int
	votedFor             map[int]int
	commitIndex          int
	lastApplied          int
	nextIndex            []int
	log                  []LogEntry
	electionTimeout      int
	appendEntriesChannel chan *AppendEntriesReply
	requestVotesChannel  chan *RequestVoteReply
	state                State
	applyCh              chan ApplyMsg
	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// fmt.Printf("server-%d:: GetState: Acquired lock.\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	// fmt.Printf("server-%d:: GetStat: Relinquished lock.\n", rf.me)

	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term           int
	CandidateIndex int
	LastLogIndex   int
	LastLogTerm    int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) candidateLogMoreUpToDate(candidateLastLogTerm int, candidateLastLogIndex int) bool {
	curServerLastLogIndex := len(rf.log) - 1
	curServerLastLogTerm := 0
	if curServerLastLogIndex >= 0 {
		curServerLastLogTerm = rf.log[curServerLastLogIndex].Term
	}
	fmt.Printf("server-%d:: RequestVote: candidateLastLogTerm: %d, candidateLastLogIndex: %d, curServerLastLogTerm: %d, curServerLastLogIndex: %d\n", rf.me, candidateLastLogTerm, candidateLastLogIndex, curServerLastLogTerm, curServerLastLogIndex)
	if candidateLastLogTerm > curServerLastLogTerm {
		return true
	} else if candidateLastLogTerm == curServerLastLogTerm {
		return candidateLastLogIndex >= curServerLastLogIndex
	}
	return false
}

func (rf *Raft) prefixMatches(leaderLastLogTerm int, leaderLastLogIndex int) bool {
	prefixMatch := true
	if leaderLastLogIndex == -1 {
		prefixMatch = true
	} else if leaderLastLogIndex >= len(rf.log) {
		prefixMatch = false
	} else {
		if rf.log[leaderLastLogIndex].Term == leaderLastLogTerm {
			prefixMatch = true
		} else {
			prefixMatch = false
		}
	}
	fmt.Printf("server-%d:: AppendEntries: leaderLastLogTerm: %d, leaderLastLogIndex: %d, prefixMatch: %t, log on server: ", rf.me, leaderLastLogTerm, leaderLastLogIndex, prefixMatch)
	fmt.Println(rf.log)

	return prefixMatch
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	defer rf.mu.Unlock()
	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		if rf.state == Candidate || rf.state == Leader {
			fmt.Printf("server-%d:: AppendEntries: More up-to-date server %d detected! Transitioning from %s -> %s.!\n", rf.me, args.LeaderID, rf.state, Follower.String())
			rf.state = Follower
		}
		// We need to reset the Election Timer here in case the log entries are null,
		// since we have received a heartbeat message from the current leader. This is
		// done by sending a messsage to the rf.appendEntriesChannel, which is consumed by
		// the election timeout goroutine to reset the timer. For now, let's assume
		// that we need to reset this election timer regardless of the type of
		// AppendEntries RPC is received (heartbeat/replication).
		rf.appendEntriesChannel <- reply
	} else {
		// RPC is received from a less up-to-date server. In this case, we should
		// reject this RPC and send our current term so that the sender can
		// update their curren term.
		fmt.Printf("server-%d:: AppendEntries from %d of term %d, my term: %d. Rejecting RPC!\n", rf.me, args.LeaderID, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Appended = false
		return
	}
	// fmt.Printf("1. server %d:: Received RPC from %d, len of args %d\n", rf.me, args.LeaderID, len(args.LogEntries))

	if len(args.LogEntries) > 0 {
		fmt.Printf("server-%d:: Received AppendEntries RPC from %d: PrevLogIndex - %d, PrevLogTerm - %d, Commit Index - %d, LogEntries: ", rf.me, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.CommitIndex)
		fmt.Println(args.LogEntries)
		if !rf.prefixMatches(args.PrevLogTerm, args.PrevLogIndex) {
			reply.Appended = false
			return
		} else {
			targetIndex := args.PrevLogIndex + 1
			if len(rf.log) > targetIndex {
				rf.log = rf.log[:targetIndex]
				fmt.Printf("server-%d:: Log after truncate: ", rf.me)
				fmt.Println(rf.log)
			}

			rf.log = append(rf.log, args.LogEntries...)
			fmt.Printf("server-%d:: Log after append: ", rf.me)
			fmt.Println(rf.log)
			reply.Appended = true
		}
	}
	if args.CommitIndex > rf.commitIndex {
		lastIndex := len(rf.log) - 1
		rf.commitIndex = int(math.Min(float64(args.CommitIndex), float64(lastIndex)))
		if rf.lastApplied+1 <= rf.commitIndex {
			fmt.Printf("server-%d:: AppendEntries handler (from %d): Applying commands from lastApplied (%d) to commitIndex (%d)!\n", rf.me, args.LeaderID, rf.lastApplied+1, rf.commitIndex)
		}
		for logIdx := rf.lastApplied + 1; logIdx <= rf.commitIndex; logIdx++ {
			rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: logIdx + 1, Command: rf.log[logIdx].Command}
		}
		rf.lastApplied = rf.commitIndex
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// fmt.Printf("server-%d:: RequestVote by %d: Acquired Lock!\n", rf.me, args.CandidateIndex)

	// defer rf.mu.Unlock()
	voteNotGrantedReason := ""
	if args.Term < rf.currentTerm {
		// fmt.Printf("Term of candidate server %d (%d) is less than my term (%d). Vote not granted not to %d for term %d!", args.CandidateIndex, args.Term, rf.currentTerm, args.CandidateIndex, args.Term)
		voteNotGrantedReason = fmt.Sprintf("Term of candidate server %d (%d) is less than my term (%d). Vote not granted not to %d for term %d!", args.CandidateIndex, args.Term, rf.currentTerm, args.CandidateIndex, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
	} else {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		val, ok := rf.votedFor[args.Term]
		if !ok || val == args.CandidateIndex {
			if rf.candidateLogMoreUpToDate(args.LastLogTerm, args.LastLogIndex) {
				rf.votedFor[args.Term] = args.CandidateIndex
				reply.VoteGranted = true
				rf.mu.Unlock()
				// fmt.Printf("BEFORE: server-%d:: Reply to %d: VoteGranted: %t\n", rf.me, args.CandidateIndex, reply.VoteGranted)
				rf.requestVotesChannel <- reply
				// fmt.Printf("AFTER: server-%d:: Reply to %d: VoteGranted: %t\n", rf.me, args.CandidateIndex, reply.VoteGranted)
			} else {
				voteNotGrantedReason = fmt.Sprintf("My logs are more up-to-date than candidate %d!", args.CandidateIndex)
				reply.VoteGranted = false
				rf.mu.Unlock()
			}
		} else {
			fmt.Printf("I have already voted for %d in term %d!", rf.votedFor[rf.currentTerm], rf.currentTerm)
			voteNotGrantedReason = fmt.Sprintf("I have already voted for %d in term %d!", rf.votedFor[rf.currentTerm], rf.currentTerm)
			reply.VoteGranted = false
			rf.mu.Unlock()
		}
	}
	if reply.VoteGranted {
		fmt.Printf("server-%d:: Vote granted to %d for term %d!\n", rf.me, args.CandidateIndex, args.Term)
	} else {
		fmt.Printf("server-%d:: Vote NOT granted to %d for term %d! %s\n", rf.me, args.CandidateIndex, args.Term, voteNotGrantedReason)
	}

	// fmt.Printf("server-%d:: Reply to %d: VoteGranted: %t\n", rf.me, args.CandidateIndex, reply.VoteGranted)

	// fmt.Printf("server-%d:: RequestVote: Relinquished Lock!\n", rf.me)

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	// fmt.Printf("Called AE on %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) performLogReplication(peers []*labrpc.ClientEnd, command interface{}, term int, index int) {
	totalNodes := len(peers)
	quorum := int(math.Ceil(float64(totalNodes) / 2))
	appended := make(chan bool, totalNodes)
	appended <- true
	stepDown := make(chan bool, totalNodes)
	for server := range rf.peers {
		rf.mu.Lock()
		if server != rf.me {
			go rf.sendAndHandleAppendEntriesReplication(index, server, appended, stepDown)
		}
		rf.mu.Unlock()
	}

	totalAppended := 0
	for {
		select {
		case <-appended:
			totalAppended++
			fmt.Printf("server-%d:: Received positive AppendEntries RPC response for term %d! Total positive responses: %d\n", rf.me, term, totalAppended)
			if totalAppended >= quorum {
				fmt.Printf("server-%d:: Successfully replicated command on a majority of servers!\n", rf.me)
				rf.mu.Lock()
				// This leader will only commmit entries if it has replicated at least one command in this term
				// on a majority of servers.
				if rf.currentTerm == term {
					rf.commitIndex = index
					if rf.lastApplied+1 <= rf.commitIndex {
						fmt.Printf("server-%d:: performLogReplication: Applying commands from lastApplied (%d) to commitIndex (%d)!\n", rf.me, rf.lastApplied+1, rf.commitIndex)
					}
					for logIdx := rf.lastApplied + 1; logIdx <= rf.commitIndex; logIdx++ {
						rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: logIdx + 1, Command: rf.log[logIdx].Command}
					}
					rf.lastApplied = index
				}
				rf.mu.Unlock()
				return
			}
		case <-stepDown:
			fmt.Printf("server-%d:: I am no longer leader!\n", rf.me)
			return
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var isLeader bool
	term := rf.currentTerm
	if rf.state == Leader {
		isLeader = true
	} else {
		isLeader = false
	}
	index := len(rf.log)
	fmt.Printf("server-%d:: Start - term: %d, isLeader: %t, index: %d,command:", rf.me, term, isLeader, index)
	fmt.Println(command)

	if isLeader {
		// Leader appends the command to it's log and starts agreement
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		go rf.performLogReplication(rf.peers, command, term, index)
	}
	return index + 1, term, isLeader
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
	// fmt.Printf("Server %d is dead\n", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) stepDownIfOutdated(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		return true
	}
	return false
}

func (rf *Raft) constructPayload(server int, decrementNextIndex bool) (*AppendEntriesArgs, *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me}
	reply := &AppendEntriesReply{}
	if decrementNextIndex {
		rf.nextIndex[server]--
	}
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.LogEntries = rf.log[prevLogIndex+1:]
	args.CommitIndex = rf.commitIndex
	args.LeaderID = rf.me
	fmt.Printf("server-%d:: sendAndHandleAppendEntriesReplication: Payload to be sent to %d: PrevLogIndex - %d, PrevLogTerm - %d, Commit Index - %d, LogEntries: ", rf.me, server, prevLogIndex, prevLogTerm, rf.commitIndex)
	fmt.Println(args.LogEntries)
	return args, reply
}

func (rf *Raft) sendAndHandleAppendEntriesReplication(index int, server int, appended chan bool, stepDown chan bool) {

	decrementNextIndex := false
	// The server shouldn't keep sending AppendEntries if it's not the leader. We do check in
	// the Start() method, whether the server is the leader when it receives the log replication
	// request. A server may be disconnected from all the other servers and may think it's the
	// leader, and so if a client sends an AppendEntries RPC request to this server, it will
	// call sendAndHandleAppendEntriesReplication and start the replication process. Now,
	// since this server is disconnected from all other servers, it will never get a majority
	// and it wouldn't be able to commit anything, even if it replicates things in a non-majority
	// number of servers. Eventually, when it connects back to the network, it will step down and
	// become a FOLLOWER and it should stop sending AppendEntries in an attempt to replicate the
	// entry. stepDownIfOutdated should ensure this, as when we reconnect to the network,
	// a majority of more up-to-date servers will reject its AppendEntries RPC.
	for {
		args, reply := rf.constructPayload(server, decrementNextIndex)
		ret := rf.sendAppendEntries(server, args, reply)
		if ret {
			_, isLeader := rf.GetState()
			if !isLeader {
				stepDown <- true
				return
			}
			if rf.stepDownIfOutdated(reply.Term) {
				fmt.Printf("server-%d:: sendAndHandleAppendEntriesReplication: More up-to-date server %d detected! Transitioning from %s -> %s.!\n", rf.me, server, rf.state, Follower.String())
				stepDown <- true
				return
			}
			if reply.Appended {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.nextIndex[server] = index + 1
				appended <- true
				return
			} else {
				decrementNextIndex = true
				fmt.Printf("server-%d:: sendAndHandleAppendEntriesReplication: Log predix match at server %d failed! Reconstructing payload and resending AppendEntries RPC!\n", rf.me, server)
			}
		} else {
			fmt.Printf("server-%d:: sendAndHandleAppendEntriesReplication: AppendEntries RPC response for term %d from server %d timed out! It is probably dead.\n", rf.me, args.Term, server)
		}
	}
}

func (rf *Raft) sendAndHandleAppendEntriesHeartbeat(term int, leaderId int, server int, commitIndex int) {
	args := &AppendEntriesArgs{Term: term, LeaderID: leaderId, CommitIndex: commitIndex}
	reply := &AppendEntriesReply{}
	// fmt.Printf("server-%d:: sendAndHandleAppendEntries: Sending AppendEntries RPC to %d!\n", rf.me, server)
	ret := rf.sendAppendEntries(server, args, reply)
	if ret {
		rf.mu.Lock()
		// fmt.Printf("server-%d:: sendAndHandleAppendEntries: Acquired Lock to send to %d!\n", rf.me, server)
		if reply.Term > rf.currentTerm {
			fmt.Printf("server-%d:: sendAndHandleAppendEntries: More up-to-date leader %d detected! Transitioning from %s -> %s.!\n", rf.me, server, rf.state, Follower.String())
			rf.currentTerm = reply.Term
			rf.state = Follower
		}
		rf.mu.Unlock()
		// fmt.Printf("server-%d:: sendAndHandleAppendEntries: Relinquished Lock to send to %d!\n", rf.me, server)
	} else {
		// fmt.Printf("server-%d:: sendAndHandleAppendEntries: AppendEntries RPC response for term %d from server %d timed out! It is probably dead.\n", rf.me, args.Term, server)
	}
}

func (rf *Raft) sendAndHandleRequestVote(term int, myIndex int, lastLogIndex int, lastLogTerm int, server int, positiveVotes chan bool, negativeVotes chan bool, killElection chan bool) {
	args := &RequestVoteArgs{Term: term, CandidateIndex: myIndex, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	reply := &RequestVoteReply{}
	ret := rf.sendRequestVote(server, args, reply)
	if !ret {
		fmt.Printf("server-%d:: RequestVote RPC response from %d for term %d timed out! It is probably dead.\n", rf.me, server, term)
	}
	rf.mu.Lock()
	// fmt.Printf("server-%d:: sendAndHandleRequestVote: Acquired Lock!\n", rf.me)

	defer rf.mu.Unlock()
	// Ensure that the term & server state is the same as when we initiate election.
	// If it's not the same, then maybe we received another AppendEntries RPC from
	// the leader of a future term and stepped down to Follower state.
	if rf.currentTerm != args.Term || rf.state != Candidate {
		killElection <- true
		return
	}
	if ret {
		if !reply.VoteGranted {
			negativeVotes <- true
		} else {
			positiveVotes <- true
		}
		fmt.Printf("server-%d:: RequestVote RPC response from %d for term %d. VotedFor: %t\n", rf.me, server, reply.Term, reply.VoteGranted)
	} else {
		// We we go into else if the RPC returns false, this only happens when
		// the network is lossy or the server that we're sending an RPC to is down.
		// more detailed explanation is in the comments of rf.sendRequestVote().
		fmt.Printf("server-%d:: RequestVote RPC response from %d for term %d timed out! It is probably dead.\n", rf.me, server, term)
		negativeVotes <- true
	}
	// fmt.Printf("server-%d:: sendAndHandleRequestVote: Relinquished Lock!\n", rf.me)

}

func (rf *Raft) performLeaderElection(peers []*labrpc.ClientEnd, myIndex int, term int, lastLogIndex int, lastLogTerm int, electionTimeout int) bool {
	fmt.Printf("server-%d:: electionTimeoutRoutine: Hit election timeout %d! Initiating election for term %d.\n", rf.me, electionTimeout, rf.currentTerm)
	totalNodes := len(peers)
	quorum := int(math.Ceil(float64(totalNodes) / 2))

	positiveVotes := make(chan bool, totalNodes)
	negativeVotes := make(chan bool, totalNodes)
	killElection := make(chan bool, totalNodes)
	for targetIndex := range peers {
		if targetIndex == myIndex {
			rf.mu.Lock()
			// fmt.Printf("server-%d:: performLeaderElection: Acquired Lock!\n", rf.me)
			rf.votedFor[term] = myIndex
			fmt.Printf("server-%d:: Vote granted to %d for term %d!\n", rf.me, rf.me, term)
			positiveVotes <- true
			rf.mu.Unlock()
			// fmt.Printf("server-%d:: performLeaderElection: Relinquished Lock!\n", rf.me)

		} else {
			go rf.sendAndHandleRequestVote(term, myIndex, lastLogIndex, lastLogTerm, targetIndex, positiveVotes, negativeVotes, killElection)
		}
	}
	totalVotes := 0
	numPositiveVotes := 0
	for {
		select {
		case <-positiveVotes:
			totalVotes++
			numPositiveVotes++
			// fmt.Printf("server-%d:: Received positive RequestVote RPC response for term %d! Total positive votes: %d\n", rf.me, term, numPositiveVotes)

			if numPositiveVotes >= quorum {
				rf.mu.Lock()
				fmt.Printf("server-%d:: I am the leader for term %d\n", rf.me, rf.currentTerm)
				rf.state = Leader
				for index := range rf.nextIndex {
					rf.nextIndex[index] = len(rf.log)
				}
				rf.mu.Unlock()
				return true
			} else if totalVotes == totalNodes {
				// All "Raft.RequestVote" RPCs have completed, but the number
				// of positive votes received aren't adequate for this server
				// to become leader.
				return false
			}
		case <-negativeVotes:
			totalVotes++
			if totalVotes == totalNodes {
				// All "Raft.RequestVote" RPCs have completed, but the number
				// of positive votes received aren't adequate for this server
				// to become leader.
				return false
			}
		case <-killElection:
			return false
		}
	}
}

func (rf *Raft) sendHeartbeatIfLeader(heartbeatTimeout int) {
	for !rf.killed() {
		// fmt.Printf("server-%d:: trying to get state\n", rf.me)
		term, isLeader := rf.GetState()
		// fmt.Printf("server-%d:: term: %d and isLeader: %t.\n", rf.me, term, isLeader)
		if isLeader {
			// fmt.Printf("server-%d:: Sending  assert authority as leader.\n", rf.me)
			for index := range rf.peers {
				if index != rf.me {
					go rf.sendAndHandleAppendEntriesHeartbeat(term, rf.me, index, rf.commitIndex)
				}
			}
		}
		time.Sleep(time.Duration(heartbeatTimeout) * time.Millisecond)
	}
}

func (rf *Raft) electionTimeoutRoutine(electionTimeout int) {
	for !rf.killed() {
		// leader = false
		// fmt.Printf("server-%d:: electionTimeoutRoutine:Entered!\n", rf.me)

		select {
		case <-rf.appendEntriesChannel:
			// fmt.Printf("server-%d:: electionTimeoutRoutine: Received AppendEntries RPC from current leader. Resetting election timeout.\n", rf.me)
			// Breaking out of the select statement causes the timeout to be
			// get reset which is important when we've received a heartbeat
			// from the current leader.
		case <-rf.requestVotesChannel:
			// fmt.Printf("server-%d:: electionTimeoutRoutine: Replied to Request Vote RPC. Resetting election timeout.\n", rf.me)
		case <-time.After(time.Duration(electionTimeout)*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond):

			_, leader := rf.GetState()
			if leader {
				break
			}

			// if !rf.killed() {
			// 	fmt.Printf("server-%d:: electionTimeoutRoutine: Just had election timeout.\n", rf.me)
			// }

			rf.mu.Lock()
			// fmt.Printf("server-%d:: electionTimeoutRoutine (timeout): Acquired lock.\n", rf.me)
			rf.currentTerm += 1
			// if !rf.killed() {
			// 	fmt.Printf("server-%d:: electionTimeoutRoutine: Hit election timeout %d! Initiating election for term %d.\n", rf.me, electionTimeout, rf.currentTerm)
			// }
			rf.state = Candidate
			curServerLastLogIndex := len(rf.log) - 1
			curServerLastLogTerm := 0
			if curServerLastLogIndex >= 0 {
				curServerLastLogTerm = rf.log[curServerLastLogIndex].Term
			}
			curTerm := rf.currentTerm
			rf.mu.Unlock()
			// fmt.Printf("server-%d:: electionTimeoutRoutine (timeout): Relinquished lock.\n", rf.me)

			if !rf.killed() {
				// Perform leader election in a Go routine so that the timer restarts and does not wait for the result of the leader election.
				// This is applicable for the case when no leader is able to get a majority as majority of the nodes are disconnected so election is not able to 'finish' i.e. we do not receive a majority of votes nor the responses from all nodes.
				// In such a case, we need to start a re-election after a timeout (which we are now doing since we have leader election in a separate go routine).
				go rf.performLeaderElection(rf.peers, rf.me, curTerm, curServerLastLogIndex, curServerLastLogTerm, electionTimeout)
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.votedFor = make(map[int]int)
	rf.appendEntriesChannel = make(chan *AppendEntriesReply)
	rf.requestVotesChannel = make(chan *RequestVoteReply)
	rf.applyCh = applyCh
	// Choosing a randomized timeout between 500 & 800 ms. This is because
	// our AppendEntries RPC that is used as heartbeat is sent once per 100ms.
	// If we haven't received even one heartbeat within 500ms, then we can declare
	// the leader as dead and:
	// increment our current term -> transition to CANDIDATE state -> initiate leader election
	// -> wait for a vote from a of of followers.
	min := 300
	max := 500
	electionTimeout := rand.Intn(max-min+1) + min
	fmt.Printf("server-%d:: Selected random timeout %d!\n", me, electionTimeout)
	heartbeatTimeout := 100
	go rf.electionTimeoutRoutine(electionTimeout)
	go rf.sendHeartbeatIfLeader(heartbeatTimeout)

	return rf
}
