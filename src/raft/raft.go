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
	matchIndex           []int
	log                  []LogEntry
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

func (rf *Raft) GetStateAndLogLength() (int, bool, int) {
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

	return term, isleader, len(rf.log)
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
	if !prefixMatch {
		fmt.Printf("server-%d:: AppendEntries: leaderLastLogTerm: %d, leaderLastLogIndex: %d, prefixMatch: %t, log on server: %v\n", rf.me, leaderLastLogTerm, leaderLastLogIndex, prefixMatch, rf.log)
	}

	return prefixMatch
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("server-%d:: AppendEntries from %d of term %d, my term: %d!\n", rf.me, args.LeaderID, args.Term, rf.currentTerm)

	// If args.Term == rf.currentTerm:
	// - If you're a leader, you should step down. This is something that shouldn't
	// happen because this means that there are two leaders for a single term. But
	// maybe we don't need to worry about it.
	// - If you're a candidate, you SHOULD step down because it means you've
	// discovered the leader of the current term.
	// If args.Term > rf.currentTerm:
	// - If you're a leader, you SHOULD step down since you've been
	//  now detected a server (rather, leader) with a higher term. This is important to
	// neutralize old leaders.
	// - If you're a candidate, you SHOULD step down since you've detected
	// a server (rather, leader) with a higher term.
	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		if rf.state == Candidate || rf.state == Leader {
			fmt.Printf("server-%d:: AppendEntries RPC Handler: Server %d has higher term (%d) than me (%d)! Transitioning from %s -> %s.!\n", rf.me, args.LeaderID, args.Term, rf.currentTerm, rf.state, Follower.String())
			rf.state = Follower
		}
		// rf.appendEntriesChannel is consumed by the election timeout goroutine
		// to reset the timer.
		rf.appendEntriesChannel <- reply
	} else {
		// RPC is received from a server with an older term. In this case, we should
		// reject this RPC and send our current term so that the sender can
		// update their current term.
		reply.Term = rf.currentTerm
		reply.Appended = false
		return
	}
	// fmt.Printf("1. server %d:: Received RPC from %d, len of args %d\n", rf.me, args.LeaderID, len(args.LogEntries))

	if !rf.prefixMatches(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Appended = false
		return
	} else {
		if len(args.LogEntries) > 0 {
			fmt.Printf("server-%d:: Received AppendEntries RPC from %d: PrevLogIndex - %d, PrevLogTerm - %d, Commit Index - %d, LogEntries: %v\n", rf.me, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.CommitIndex, args.LogEntries)
			appendStartIndex := args.PrevLogIndex + 1
			// Only truncate future logs if there's a conflict at the appendStartIndex.
			if appendStartIndex < len(rf.log) && rf.log[appendStartIndex].Term != args.LogEntries[0].Term {
				rf.log = rf.log[:appendStartIndex]
				fmt.Printf("server-%d:: Log after truncate: %v\nc", rf.me, rf.log)
			}
			index := 0
			for index < len(args.LogEntries) && appendStartIndex < len(rf.log) {
				// fmt.Printf("server-%d:: Appending %d from args.LogEntries (length %d)  at index %d (length %d)\n", rf.me, index, len(args.LogEntries), appendStartIndex, len(rf.log))
				rf.log[appendStartIndex] = args.LogEntries[index]
				appendStartIndex++
				index++
			}
			if index < len(args.LogEntries) {
				// fmt.Printf("server-%d:: Appending slice from index %d:%d from args.LogEntries to rf.log\n", rf.me, index, len(args.LogEntries))
				rf.log = append(rf.log, args.LogEntries[index:]...)
			}
			fmt.Printf("server-%d:: Log after append: %v\n", rf.me, rf.log)
		}
		reply.Appended = true
	}
	if args.CommitIndex > rf.commitIndex {
		lastIndex := len(rf.log) - 1
		rf.commitIndex = int(math.Min(float64(args.CommitIndex), float64(lastIndex)))
		for logIdx := rf.lastApplied + 1; logIdx <= rf.commitIndex; logIdx++ {
			// fmt.Printf("server-%d:: performLogReplication: Applying command at index (%d)!\n", rf.me, logIdx)
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
		// If args.Term == rf.currentTerm:
		// - If you're a leader, you shouldn't step down since you've been
		//  elected as the leader for that term.
		// - If you're a candidate, you don't need
		// to step down since multiple candidates can exist for a single term.
		// If args.Term > rf.currentTerm:
		// - If you're a leader, you SHOULD step down since you've been
		//  now detected a server with a higher term. This is important to
		// neutralize old leaders.
		// - If you're a candidate, you SHOULD step down since you've detected
		// a server with a higher term.
		reply.Term = args.Term
		rf.currentTerm = args.Term
		if rf.state == Leader || rf.state == Candidate {
			fmt.Printf("server-%d:: RequestVote RPC Handler: Server %d has higher term (%d) than me (%d)! Transitioning from %s -> %s.!\n", rf.me, args.CandidateIndex, args.Term, rf.currentTerm, rf.state, Follower.String())
			rf.state = Follower
		}
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

func (rf *Raft) sendHeartbeatIfLeader(heartbeatTimeout int) {
	for !rf.killed() {
		// fmt.Printf("server-%d:: trying to get state\n", rf.me)
		_, isLeader, logLength := rf.GetStateAndLogLength()
		// fmt.Printf("server-%d:: sendHeartbeatIfLeader: term: %d and isLeader: %t.\n", rf.me, term, isLeader)
		if isLeader {
			// fmt.Printf("server-%d:: Sending assert authority as leader.\n", rf.me)
			for server := range rf.peers {
				if server != rf.me {
					go rf.sendAndHandleAppendEntries(logLength-1, server, nil, nil, true)
				}
			}
		}
		time.Sleep(time.Duration(heartbeatTimeout) * time.Millisecond)
	}
}

func (rf *Raft) performLogReplication(peers []*labrpc.ClientEnd, command interface{}, term int, index int, leaderID int) {
	totalNodes := len(peers)
	quorum := int(math.Ceil(float64(totalNodes) / 2))
	appended := make(chan bool, totalNodes)
	appended <- true
	stepDown := make(chan bool, totalNodes)
	for server := range peers {
		if server != leaderID {
			go rf.sendAndHandleAppendEntries(index, server, appended, stepDown, false)
		}
	}

	totalAppended := 0
	for {
		select {
		case <-appended:
			totalAppended++
			// fmt.Printf("server-%d:: Received positive AppendEntries RPC response for term %d! Total positive responses: %d\n", rf.me, term, totalAppended)
			if totalAppended >= quorum {
				fmt.Printf("server-%d:: Successfully replicated command at index %d a majority of servers!\n", rf.me, index)
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
	index := rf.nextIndex[rf.me]

	if isLeader {
		// Leader appends the command to it's log and starts agreement
		fmt.Printf("server-%d:: Start - term: %d, isLeader: %t, index: %d,command:", rf.me, term, isLeader, index)
		fmt.Println(command)
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		fmt.Printf("server-%d:: Leader logs after append: %v\n", rf.me, rf.log)
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
		rf.nextIndex[rf.me]++
		go rf.performLogReplication(rf.peers, command, term, index, rf.me)
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
	// One thing that we need to think about is this: Is it safe for us
	// to just compare args.Term & reply.Term here and step down if
	// args.Term is smaller?
	// What we're currently doing is, we're comparing
	// reply.Term with rf.currentTerm and only stepping down IF WE ARE STILL
	// ON AN OLDER TERM. To me, this makes sense since our currentTerm is already
	// more up-to-date than the RPC reply we, don't need to do anything since
	// we have updated our term elsewhere (possibly in the RequestVote & AppendEntries handler),
	// and have already transitioned to the Follower state. Question is, is it okay to
	// assume that?
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		return true
	}
	return false
}

func (rf *Raft) constructPayload(term int, targetIndex int, server int, decrementNextIndex bool, _type string) (*AppendEntriesArgs, *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("server-%d:: %s Append Entries: Constructing Payload to be sent to %d: targetIndex - %d\n ", rf.me, _type, server, targetIndex)

	args := &AppendEntriesArgs{Term: term, LeaderID: rf.me}
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

	if rf.nextIndex[server] <= len(rf.log) {
		args.LogEntries = rf.log[rf.nextIndex[server]:len(rf.log)]
	} else {
		args.LogEntries = make([]LogEntry, 0)
	}
	// fmt.Printf("server-%d:: %s Append Entries to %d:  nextIndex - %d, targetIndex - %d\n", rf.me, _type, server, rf.nextIndex[server], targetIndex)
	args.CommitIndex = rf.commitIndex
	args.LeaderID = rf.me
	// fmt.Printf("server-%d:: %s Append Entries: Payload to be sent to %d: %+v\n", rf.me, _type, server, args)
	return args, reply
}

func (rf *Raft) sendAndHandleAppendEntries(index int, server int, appended chan bool, stepDown chan bool, heartbeat bool) {
	decrementNextIndex := false
	// The server shouldn't keep sending AppendEntries if it's not the leader. We do check in
	// the Start() method whether the server is the leader when it receives the log replication
	// request. A server may be disconnected from all the other servers and may think it's the
	// leader, and so if a client sends an AppendEntries RPC request to this server, it will
	// call sendAndHandleAppendEntries and start the replication process. Now,
	// since this server is disconnected from all other servers, it will never get a majority
	// and it wouldn't be able to commit anything, even if it replicates things in a non-majority
	// number of servers. Eventually, when it connects back to the network, it will step down and
	// become a FOLLOWER and it should stop sending AppendEntries in an attempt to replicate the
	// entry. stepDownIfOutdated should ensure this, as when we reconnect to the network,
	// a majority of more up-to-date servers will reject its AppendEntries RPC.
	_type := "REPLICATION"
	if heartbeat {
		_type = "HEARTBEAT"
	}
	for {
		term, isLeader := rf.GetState()
		if !isLeader {
			if !heartbeat {
				stepDown <- true
			}
			return
		}
		args, reply := rf.constructPayload(term, index, server, decrementNextIndex, _type)
		ret := rf.sendAppendEntries(server, args, reply)

		if ret {
			if rf.stepDownIfOutdated(reply.Term) {
				fmt.Printf("server-%d:: %s Append Entries: More up-to-date server %d detected! Transitioning from %s -> %s.!\n", rf.me, _type, server, rf.state, Follower.String())
				if !heartbeat {
					stepDown <- true
				}
				return
			}
			if reply.Appended {
				if len(args.LogEntries) > 0 {
					rf.mu.Lock()
					totalNodes := len(rf.peers)
					quorum := int(math.Ceil(float64(totalNodes) / 2))
					// fmt.Printf("server-%d:: %s Append Entries response from %d : Log entries appended: ", rf.me, _type, server)
					// fmt.Println(args.LogEntries)
					appendEndIndex := args.PrevLogIndex + len(args.LogEntries)
					rf.nextIndex[server] = appendEndIndex + 1
					rf.matchIndex[server] = appendEndIndex
					// fmt.Printf("server-%d:: %s Append Entries response from %d : Log entries appended: %v, matchIndex: %d, nextIndex: %d\n", rf.me, _type, server, args.LogEntries, rf.nextIndex[server], rf.nextIndex[server]+1)

					numNodes := 0
					potentialCommitIndex := math.MaxInt32
					for index := range rf.matchIndex {
						if rf.matchIndex[index] > rf.commitIndex {
							potentialCommitIndex = int(math.Min(float64(rf.matchIndex[index]), float64(potentialCommitIndex)))
							numNodes++
						}
					}
					if numNodes >= quorum && rf.log[potentialCommitIndex].Term == rf.currentTerm {
						rf.commitIndex = potentialCommitIndex
						// fmt.Printf("server-%d:: %s Append Entries response from %d : Setting commitIndex to: %d\n", rf.me, _type, server, rf.commitIndex)
						for logIdx := rf.lastApplied + 1; logIdx <= rf.commitIndex; logIdx++ {
							// fmt.Printf("server-%d:: %s Append Entries response from %d: Applying command at index (%d)!\n", rf.me, _type, server, logIdx)
							rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: logIdx + 1, Command: rf.log[logIdx].Command}
						}
						rf.lastApplied = rf.commitIndex
					}
					rf.mu.Unlock()
				}
				if !heartbeat {
					appended <- true
				}
				return
			} else {
				decrementNextIndex = true
				fmt.Printf("server-%d:: %s Append Entries response from %d: Log prefix match failed! Reconstructing payload and resending AppendEntries RPC!\n", rf.me, _type, server)
			}
		} else {
			fmt.Printf("server-%d:: %s Append Entries: AppendEntries RPC response for term %d from server %d timed out! It is probably dead.\n", rf.me, _type, args.Term, server)
			time.Sleep(time.Duration(500) * time.Millisecond)
			if heartbeat {
				return
			}
		}
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

func (rf *Raft) electionTimeoutRoutine(electionTimeout int) {
	for !rf.killed() {
		// leader = false
		// fmt.Printf("server-%d:: electionTimeoutRoutine:Entered!\n", rf.me)

		select {
		case <-rf.appendEntriesChannel:
			// fmt.Printf("server-%d:: electionTimeoutRoutine: Received AppendEntries RPC from current leader. Resetting election timeout.\n", rf.me)
		case <-rf.requestVotesChannel:
			// fmt.Printf("server-%d:: electionTimeoutRoutine: Replied to Request Vote RPC. Resetting election timeout.\n", rf.me)
		case <-time.After(time.Duration(electionTimeout)*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond):

			_, leader := rf.GetState()
			if leader {
				break
			}

			rf.mu.Lock()
			// fmt.Printf("server-%d:: electionTimeoutRoutine (timeout): Acquired lock.\n", rf.me)
			rf.currentTerm += 1
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
	rf.matchIndex = make([]int, len(peers))
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
