package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
//import "labgob"

var DebugMode = true

func Logf(format string, args ...interface{}) {
	if !DebugMode {
		return
	}
	plusTimeArgs := append([]interface{}{time.Now()}, args...)
	fmt.Printf("%v:"+format, plusTimeArgs...)
}

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	LogEntries  []LogEntry
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role            int
	persistentState PersistentState
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int

	lastReceivedTime time.Time
	applyCh          chan ApplyMsg
	Stop             bool
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.persistentState.CurrentTerm
	isleader = rf.role == LEADER
	return term, isleader
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persistentState)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var raftState PersistentState
	if d.Decode(&raftState) != nil {
		log.Fatal("Decode Persister failed")
	} else {
		rf.persistentState = raftState
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	Logf("server %d(term:%d) received request vote from server %d(term:%d)\n",
		rf.me, rf.persistentState.CurrentTerm, args.CandidateId, args.Term)
	granted := false
	// If current term is less than args.term or hasn't voted for anyone except the one requests vote
	if rf.persistentState.CurrentTerm < args.Term || (rf.persistentState.CurrentTerm == args.Term &&
		(rf.persistentState.VotedFor == -1 || rf.persistentState.VotedFor == args.CandidateId)) {
		rf.persistentState.CurrentTerm = args.Term
		rf.role = FOLLOWER
		if len(rf.persistentState.LogEntries) == 0 {
			granted = true
		} else {
			logEntries := rf.persistentState.LogEntries
			lastLogEntry := logEntries[len(logEntries)-1]

			if lastLogEntry.Term < args.LastLogTerm {
				granted = true
			} else if lastLogEntry.Term > args.LastLogTerm {
				granted = false
			} else {
				granted = lastLogEntry.Index <= args.LastLogIndex
			}
		}
	}
	if granted {
		//rf.persistentState.CurrentTerm = args.Term
		rf.lastReceivedTime = time.Now()
		rf.persistentState.VotedFor = args.CandidateId
		reply.VoteGranted = true
	}else{
		reply.VoteGranted = false
	}

	rf.persist()
	reply.Term = rf.persistentState.CurrentTerm
	Logf("in (server %d) vote server %d, result:%v\n", rf.me, args.CandidateId, reply)
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeadId       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Index   int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("server %d received heartbeat from server %d\n",rf.me, args.LeadId)
	//rf.timerCh <- time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) != 0 {
		Logf("server %d(term:%d) received entries[%d:%d] from server %d(term:%d)\n", rf.me,
			rf.persistentState.CurrentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), args.LeadId, args.Term)
		Logf("in server %d, args.PrevLogIndex:%d, args.PrevLogTerm:%d, len(LogEntries):%d\n",
			rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.persistentState.LogEntries))
	}

	success := true
	if rf.persistentState.CurrentTerm > args.Term {
		success = false
		goto appendEntriesFinally
	}
	rf.lastReceivedTime = time.Now()
	rf.role = FOLLOWER
	// new term
	if rf.persistentState.CurrentTerm != args.Term {
		rf.persistentState.VotedFor = -1
	}
	rf.persistentState.CurrentTerm = args.Term
	rf.persist()
	if args.PrevLogIndex >= 1{
		if len(rf.persistentState.LogEntries) < args.PrevLogIndex{
			success = false
			reply.Index = len(rf.persistentState.LogEntries) + 1
		}else if rf.persistentState.LogEntries[args.PrevLogIndex-1].Term != args.PrevLogTerm{
			success = false
			term := rf.persistentState.LogEntries[args.PrevLogIndex-1].Term
			minIndex := 1
			maxIndex := args.PrevLogIndex
			for minIndex + 1 < maxIndex {
				midIndex := (minIndex + maxIndex) >> 1
				if rf.persistentState.LogEntries[midIndex-1].Term == term{
					maxIndex = midIndex
				}else{
					minIndex = maxIndex
				}
			}
			if rf.persistentState.LogEntries[minIndex-1].Term == term{
				reply.Index = minIndex
			}else{
				reply.Index = maxIndex
			}
		}
	}
	if success {
		for i, entry := range args.Entries {
			index := entry.Index
			// if the entry has been committed, skip this entry
			if index <= rf.commitIndex{
				continue
			}
			if len(rf.persistentState.LogEntries) >= index {
				if rf.persistentState.LogEntries[index-1].Term == entry.Term {
					continue
				}
				// delete existing conflict entries
				rf.persistentState.LogEntries = append(rf.persistentState.LogEntries[:index-1],
					rf.persistentState.LogEntries[len(rf.persistentState.LogEntries):]...)
			}
			rf.persistentState.LogEntries = append(rf.persistentState.LogEntries, args.Entries[i:]...)
			rf.persist()
			break
		}
		if len(rf.persistentState.LogEntries) > 0 {
			lastLogEntry := rf.persistentState.LogEntries[len(rf.persistentState.LogEntries)-1]
			Logf("Server %d, lastLogEntry:%v\n", rf.me, lastLogEntry)
		}
		prevCommitted := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.persistentState.LogEntries) {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.persistentState.LogEntries)
			}
			Logf("in (Follower)server %d, args.PrevLogIndex:%d, args.PrevLogTerm:%d\n",
				rf.me, args.PrevLogIndex, args.PrevLogTerm)
			for index := prevCommitted + 1; index <= rf.commitIndex; index++ {
				Logf("(Follower)server %d commit index:%d\n", rf.me, index)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.persistentState.LogEntries[index-1].Command,
					CommandIndex: index,
				}
				rf.lastApplied = index
			}
		}
	}

appendEntriesFinally:
	reply.Term = rf.persistentState.CurrentTerm
	reply.Success = success
	Logf("reply from server %d:%v\n",rf.me, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = rf.role == LEADER

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	Logf("Leader %d(term:%d) received command:%d, now nextIndex:%v\n",
		rf.me, rf.persistentState.CurrentTerm, command.(int), rf.nextIndex)
	index = len(rf.persistentState.LogEntries) + 1
	term = rf.persistentState.CurrentTerm
	rf.persistentState.LogEntries = append(rf.persistentState.LogEntries, LogEntry{
		Term:    rf.persistentState.CurrentTerm,
		Index:   index,
		Command: command,
	})
	rf.persist()
	rf.mu.Unlock()
	rf.syncWithFollowers()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off DebugMode output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Stop = true

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
	fmt.Printf("Start make server %d\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.role = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.persistentState.VotedFor = -1
	rf.applyCh = applyCh
	rf.lastReceivedTime = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.leaderCron()
	go rf.nonLeaderCron()
	return rf
}

func (rf *Raft) leaderCron() {
	heartbeatsInterval := 100 * time.Millisecond
	for {
		rf.mu.Lock()
		if rf.Stop {
			rf.mu.Unlock()
			break
		}
		if rf.role == LEADER {
			rf.mu.Unlock()
			go rf.syncWithFollowers()
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(heartbeatsInterval)

	}
}

func (rf *Raft) nonLeaderCron() {
	me := rf.me
	electionTimeoutBase := 200 * time.Millisecond
	for {
loop:
		rf.mu.Lock()
		if rf.Stop {
			rf.mu.Unlock()
			break
		}
		timeSinceLastReceived := time.Since(rf.lastReceivedTime)
		rf.mu.Unlock()
		electionTimeout := electionTimeoutBase + time.Duration(rand.Intn(200))*time.Millisecond
		sleepTime := electionTimeout - timeSinceLastReceived
		if sleepTime > 0 {
			time.Sleep(sleepTime)
		}
startRequestVote:
		rf.mu.Lock()
		timeSinceLastReceived = time.Since(rf.lastReceivedTime)
		// if this server is leader or has received packets in electionTimeout, no need to go forward.
		if rf.role == LEADER || timeSinceLastReceived < electionTimeout {
			rf.mu.Unlock()
			continue
		}
		Logf("%dms since last packet received, electionTimeout is %dms\n",
			timeSinceLastReceived.Milliseconds(), electionTimeout.Milliseconds())
		// reset timer
		electionTimeout = electionTimeoutBase + time.Duration(rand.Intn(200))*time.Millisecond

		rf.role = CANDIDATE
		rf.persistentState.CurrentTerm++
		// increase CurrentTerm, set vote to self
		rf.persistentState.VotedFor = rf.me
		rf.persist()
		voteArgs := &RequestVoteArgs{
			Term:        rf.persistentState.CurrentTerm,
			CandidateId: me,
		}
		Logf("server %d now is candidate, term:%d\n", me, rf.persistentState.CurrentTerm)
		if len(rf.persistentState.LogEntries) > 0 {
			logEntries := rf.persistentState.LogEntries
			lastLogEntry := logEntries[len(logEntries)-1]
			voteArgs.LastLogIndex = lastLogEntry.Index
			voteArgs.LastLogTerm = lastLogEntry.Term
		} else {
			voteArgs.LastLogIndex = -1
			voteArgs.LastLogTerm = -1
		}
		//if rf.persistentState.VotedFor != -1{
		//	rf.mu.Unlock()
		//	continue
		//}
		rf.lastReceivedTime = time.Now()
		rf.mu.Unlock()
		voteReplyCh := make(chan *RequestVoteReply, len(rf.peers))
		go func() {
			defer close(voteReplyCh)
			wg := sync.WaitGroup{}
			for id := range rf.peers {
				if id == me {
					continue
				}
				serverId := id
				wg.Add(1)
				go func() {
					defer wg.Done()
					reply := &RequestVoteReply{}
					// no need add timeout around sending rpc request because labrpc has it.
					rf.sendRequestVote(serverId, voteArgs, reply)
					voteReplyCh <- reply
				}()
			}
			wg.Wait()
		}()


		approvedCount := 1
		disApprovedCount := 0
		for {
			rf.mu.Lock()
			if time.Since(rf.lastReceivedTime) >= electionTimeout{
				rf.mu.Unlock()
				goto startRequestVote
			}
			rf.mu.Unlock()
			select {
			case voteResult := <- voteReplyCh:
				Logf("in server %d election, drained from voteReplyCh, waiting for lock.\n", rf.me)
				rf.mu.Lock()
				Logf("in server %d election, drained from voteReplyCh, got lock.\n", rf.me)
				if voteResult.VoteGranted {
					approvedCount++
					Logf("in server %d election, approvedCount:%d, total servers:%d\n",
						rf.me, approvedCount, len(rf.peers))
				} else {
					disApprovedCount++
					if voteResult.Term > rf.persistentState.CurrentTerm {
						rf.persistentState.CurrentTerm = voteResult.Term
						rf.persist()
					}
				}
				if approvedCount > len(rf.peers)-approvedCount {
					rf.role = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for id := range rf.peers {
						if id != rf.me {
							rf.nextIndex[id] = len(rf.persistentState.LogEntries) + 1
							rf.matchIndex[id] = 0
						}
					}
					Logf("server %d turn to be leader, term:%d, nextIndex:%v, btw, release lock.\n",
						rf.me, rf.persistentState.CurrentTerm, rf.nextIndex)
					rf.mu.Unlock()
					go rf.syncWithFollowers()
					goto drainOutVoteCh
				} else if disApprovedCount > len(rf.peers)-disApprovedCount {
					rf.role = FOLLOWER
					Logf("server %d failed in election, now is follower, term:%d, btw, release lock.\n",
						rf.me, rf.persistentState.CurrentTerm)
					rf.mu.Unlock()
					goto drainOutVoteCh
				}
				Logf("in server %d election, drained from voteReplyCh, release lock.\n", rf.me)
				rf.mu.Unlock()
			default:
				rf.mu.Lock()
				if rf.role == FOLLOWER{
					rf.mu.Unlock()
					goto loop
				}
				rf.mu.Unlock()
			}
		}
drainOutVoteCh:
		// drain out voteReplyCh
		go func() {
			for _ = range voteReplyCh {
				Logf("drain out voteReplyCh\n")
			}
		}()
	}
}

func (rf *Raft) syncWithFollowers() {
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	syncReplyCh := make(chan bool, len(rf.peers))
	go func() {
		defer close(syncReplyCh)
		wg := sync.WaitGroup{}
		for id := range rf.peers {
			if id != rf.me {
				serverId := id
				wg.Add(1)
				go func() {
					defer wg.Done()
					syncResult := rf.syncWithFollower(serverId)
					syncReplyCh <- syncResult
				}()
			}
		}
		wg.Wait()
	}()

	successCount := 1
	failCount := 0
	start := time.Now()
	for time.Since(start) < 50 * time.Millisecond{
		select {
		case reply := <- syncReplyCh:
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if !reply {
				failCount++
				continue
			}
			successCount++
			if successCount > len(rf.peers)-successCount {
				rf.updateLeaderCommitIndex()
				break
			} else if failCount > len(rf.peers)-failCount {
				break
			}
		default:

		}
	}

	// drain out syncReplyCh
	go func() {
		for _ = range syncReplyCh {
		}
	}()
}

func (rf *Raft) syncWithFollower(server int) bool {
	reply := &AppendEntriesReply{}
	now := time.Now()

	for !reply.Success && time.Since(now) < 30*time.Millisecond {
		requestId := time.Now().UnixNano()
		rf.mu.Lock()
		appendArgs := &AppendEntriesArgs{
			Term:         rf.persistentState.CurrentTerm,
			LeadId:       rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			LeaderCommit: rf.commitIndex,
		}
		if appendArgs.PrevLogIndex != len(rf.persistentState.LogEntries){
			// attention: DON'T assign the slice(rf.persistentState.LogEntries) directly to appendArgs.Entries
			// because appendArgs in sendAppendEntries will read this slice,
			// if AppendEntries is writing rf.persistentState.LogEntries, will cause data race
			appendArgs.Entries = append([]LogEntry{}, rf.persistentState.LogEntries[rf.nextIndex[server]-1:]...)
		}
		if appendArgs.PrevLogIndex > 0 {
			appendArgs.PrevLogTerm = rf.persistentState.LogEntries[appendArgs.PrevLogIndex-1].Term
		} else {
			appendArgs.PrevLogTerm = 0
		}
		if rf.role != LEADER {
			rf.mu.Unlock()
			return false
		}
		Logf("[requestID=%v]server %d(term:%d) send entries[%d~%d] to server %d\n",
			requestId, rf.me, rf.persistentState.CurrentTerm, rf.nextIndex[server], len(rf.persistentState.LogEntries), server)

		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, appendArgs, reply)
		if !ok{
			continue
		}
		Logf("SyncEntries reply of request(%v) from server %d: %v\n", requestId, server, reply)
		rf.mu.Lock()
		rf.lastReceivedTime = time.Now()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return false
		}
		if reply.Success {
			rf.matchIndex[server] = appendArgs.PrevLogIndex + len(appendArgs.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			Logf("leader %d, change nextIndex of server%d, then nextIndex:%v, matchIndex:%v\n",
				rf.me, server, rf.nextIndex, rf.matchIndex)
			rf.mu.Unlock()
			return true
		}
		if reply.Term > rf.persistentState.CurrentTerm {
			rf.persistentState.CurrentTerm = reply.Term
			rf.role = FOLLOWER
			rf.persist()
			Logf("leader %d, turn to be follower(term:%d)\n", rf.me, rf.persistentState.CurrentTerm)
			rf.mu.Unlock()
			return false
		}
		//if rf.nextIndex[server] > 1 {
		//	rf.nextIndex[server]--
		//}
		if reply.Index > 0{
			rf.nextIndex[server] = reply.Index
		}

		rf.mu.Unlock()
	}
	return false
}

func (rf *Raft) updateLeaderCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.persistentState.LogEntries) == 0 {
		return
	}
	committed := rf.commitIndex
	min := rf.commitIndex
	max := rf.commitIndex
	newCommitted := committed
	Logf("Leader: %d,current commit index:%d,  matchIndex:%v, nextIndex:%v\n",
		rf.me, rf.commitIndex, rf.matchIndex, rf.nextIndex)
	for _, match := range rf.matchIndex {
		if match > max {
			max = match
		}
	}
	if max == committed {
		return
	}
	for min+1 < max {
		mid := (min + max) >> 1
		if isMajorityMatch(rf.matchIndex, mid) {
			min = mid
		} else {
			max = mid
		}
	}
	//fmt.Println("in update commitIndex", max, rf.persistentState.LogEntries)
	if isMajorityMatch(rf.matchIndex, max) {
		newCommitted = max
	} else {
		newCommitted = min
	}
	if newCommitted == committed || rf.persistentState.LogEntries[newCommitted-1].Term != rf.persistentState.CurrentTerm {
		return
	}
	rf.commitIndex = newCommitted
	Logf("Leader, last commitIndex:%d, new commitIndex:%d\n", committed, rf.commitIndex)
	if committed != rf.commitIndex {
		for index := committed + 1; index <= rf.commitIndex; index++ {
			Logf("(Leader)server %d commit index:%d\n", rf.me, index)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.persistentState.LogEntries[index-1].Command,
				CommandIndex: index,
			}
			rf.lastApplied = index
		}
	}
}

func isMajorityMatch(matchIndex []int, index int) bool {
	count := 1
	for _, match := range matchIndex {
		if match >= index {
			count++
		}
	}
	return count > len(matchIndex)-count
}
