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

const (
	HeartBeat = iota
	SyncEntries
)

type LogEntry struct {
	Term  int
	Index int
	Command interface{}
}

type RaftState struct {
	currentTerm int
	votedFor int
	logEntries []LogEntry
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
	flag              int
	raftState         RaftState
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int

	lastReceivedTime time.Time
	applyCh          chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.raftState.currentTerm
	isleader = rf.flag == LEADER
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
	e.Encode(rf.raftState)
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
	var raftState RaftState
	if d.Decode(&raftState) != nil{
		log.Fatal("Decode Persister failed")
	}else{
		rf.raftState = raftState
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Printf("server %d received request vote from server %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastReceivedTime = time.Now()
	//fmt.Println("server change lastReceivedTime", rf.lastReceivedTime)
	if rf.raftState.currentTerm < args.Term ||(rf.raftState.currentTerm==args.Term&&
		(rf.raftState.votedFor == -1 ||rf.raftState.votedFor == args.CandidateId)){
		approveCondition := false
		if len(rf.raftState.logEntries)==0{
			approveCondition = true
		}else{
			logEntries := rf.raftState.logEntries
			lastLogEntry := logEntries[len(logEntries)-1]
			if lastLogEntry.Term <= args.Term&&lastLogEntry.Index <= args.LastLogIndex{
				approveCondition = true
			}
		}
		if approveCondition{
			rf.raftState.currentTerm = args.Term
			rf.raftState.votedFor = args.CandidateId
			reply.VoteGranted = true
			return
		}
	}
	reply.Term = rf.raftState.currentTerm
	reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, voteReplyCh chan *RequestVoteReply) bool {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	voteReplyCh <- reply
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
	Success bool
}

func (rf *Raft)AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	//fmt.Printf("server %d received heartbeat from server %d\n",rf.me, args.LeadId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries)!=0{
		fmt.Printf("server %d received entries[%d:] from server %d\n",rf.me, args.PrevLogIndex+1, args.LeadId)
	}

	rf.lastReceivedTime = time.Now()
	success := true
	if rf.raftState.currentTerm > args.Term {
		success = false
	}else{
		if args.PrevLogIndex-1>=0 && (len(rf.raftState.logEntries) < args.PrevLogIndex-1 ||
			rf.raftState.logEntries[args.PrevLogIndex-1].Term !=args.PrevLogTerm) {
			success = false
		}
	}
	if success {
		rf.flag = FOLLOWER
		// new term
		if rf.raftState.currentTerm != args.Term{
			rf.raftState.votedFor = -1
		}
		rf.raftState.currentTerm = args.Term
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if len(rf.raftState.logEntries)>index{
				if rf.raftState.logEntries[index-1].Term ==entry.Term {
					continue
				}
				// delete existing conflict entries
				rf.raftState.logEntries = append(rf.raftState.logEntries[:index-1],
					rf.raftState.logEntries[len(rf.raftState.logEntries):]...)
			}
			rf.raftState.logEntries = append(rf.raftState.logEntries, args.Entries[i:]...)
			break
		}
		prevCommitted := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex{
			if args.LeaderCommit < len(rf.raftState.logEntries){
				rf.commitIndex = args.LeaderCommit
			}else{
				rf.commitIndex = len(rf.raftState.logEntries)
			}
			for index := prevCommitted+1; index <=rf.commitIndex; index++{
				rf.applyCh<-ApplyMsg{
					CommandValid: true,
					Command:      rf.raftState.logEntries[index-1].Command,
					CommandIndex: index,
				}
			}
		}
	}
	reply.Term = rf.raftState.currentTerm
	reply.Success = success
	if len(args.Entries)!=0{
		fmt.Printf("reply from server %d:%v\n",rf.me, reply)
	}

}

func (rf *Raft)sendAppendEntries(server int, args *AppendEntriesArgs, replyCh chan *AppendEntriesReply) bool {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	replyCh <- reply
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
	isLeader = rf.flag == LEADER
	rf.mu.Unlock()
	if !isLeader{
		return index, term, isLeader
	}

	rf.mu.Lock()
	index = len(rf.raftState.logEntries)+1
	term = rf.raftState.currentTerm
	rf.raftState.logEntries = append(rf.raftState.logEntries, LogEntry{
		Term:    rf.raftState.currentTerm,
		Index:   index,
		Command: command,
	})
	rf.mu.Unlock()
	go rf.syncWithFollowers(SyncEntries)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.flag = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastReceivedTime = time.Now()
	rf.raftState.votedFor = -1
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = len(rf.raftState.logEntries)
	rf.lastApplied = len(rf.raftState.logEntries)

	go rf.leaderCron()
	go rf.nonLeaderCron()
	return rf
}

func (rf *Raft)leaderCron()  {
	heartbeatsInterval := 100 * time.Millisecond
	for{
		rf.mu.Lock()
		if rf.flag == LEADER{
			rf.mu.Unlock()
			rf.syncWithFollowers(HeartBeat)
			rf.updateLeaderCommitIndex()
		}else{
			rf.mu.Unlock()
		}

		time.Sleep(heartbeatsInterval)

	}
}

func (rf *Raft)nonLeaderCron()  {
	me := rf.me
	electionTimeout := 200 * time.Millisecond
	for{
loop:
		rf.mu.Lock()
		timeSinceLastHeartBeat := time.Since(rf.lastReceivedTime)
		rf.mu.Unlock()
		//fmt.Printf("server %d timeSinceLastHeartBeat:%fs\n", rf.me, timeSinceLastHeartBeat.Seconds())
		sleepTime := electionTimeout - timeSinceLastHeartBeat + time.Duration(rand.Intn(200))*time.Millisecond
		if sleepTime>0{
			time.Sleep(sleepTime)
			//fmt.Printf("server %d awake from sleep %fs\n",rf.me, sleepTime.Seconds())
		}
		rf.mu.Lock()
		timeSinceLastHeartBeat = time.Since(rf.lastReceivedTime)
		if rf.flag==LEADER || timeSinceLastHeartBeat < electionTimeout {
			rf.mu.Unlock()
			//fmt.Printf("server %d, keep continue to sleep in nonleaderCron\n", rf.me)
			continue
		}
		rf.mu.Unlock()
requestVote:
		rf.mu.Lock()
		rf.flag = CANDIDATE
		rf.raftState.currentTerm ++
		// increase currentTerm, reset votedFor to -1 means hasn't vote for anyone in current term
		rf.raftState.votedFor = -1
		rf.mu.Unlock()

		voteArgs := &RequestVoteArgs{
			Term:         rf.raftState.currentTerm,
			CandidateId:  me,
		}
		//fmt.Printf("Server %d now is candidate, term:%d\n", me, rf.raftState.currentTerm)
		if len(rf.raftState.logEntries)>0{
			logEntries := rf.raftState.logEntries
			lastLogEntry := logEntries[len(logEntries)-1]
			voteArgs.LastLogIndex = lastLogEntry.Index
			voteArgs.LastLogTerm = lastLogEntry.Term
		}else{
			voteArgs.LastLogIndex = -1
			voteArgs.LastLogTerm = 0
		}
		//reply := make([]*RequestVoteReply, 0)
		//for i:=0; i<len(rf.peers); i++{
		//	reply = append(reply, &RequestVoteReply{})
		//}
		rf.mu.Lock()
		if rf.raftState.votedFor != -1{
			rf.mu.Unlock()
			goto loop
		}
		rf.mu.Unlock()
		voteReplyCh := make(chan *RequestVoteReply)
		for id := range rf.peers{
			if id == me{
				continue
			}
			go rf.sendRequestVote(id, voteArgs, voteReplyCh)
		}
		approved := 1
		replyCount := 1
		fmt.Printf("now server %d wait vote result\n",rf.me)
		for{
			select {
			case reply := <-voteReplyCh:
				fmt.Printf("Server %d got vote result:%v\n", rf.me, reply)
				replyCount ++
				if reply.VoteGranted{
					approved ++
				}
				if approved > len(rf.peers) - approved{
					rf.mu.Lock()
					fmt.Printf("Server %d turn to be leader\n", rf.me)
					rf.flag = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for id := range rf.peers{
						if id != rf.me{
							rf.nextIndex[id] = len(rf.raftState.logEntries)+1
							rf.matchIndex[id] = 0
						}
					}
					rf.mu.Unlock()
					go rf.syncWithFollowers(HeartBeat)
					goto loop
				}else if replyCount == len(rf.peers){
					rf.mu.Lock()
					rf.flag = FOLLOWER
					rf.mu.Unlock()
					goto loop
				}
			case <-time.After(electionTimeout):
				goto requestVote
			}
		}

	}
}


func (rf *Raft)syncWithFollowers(syncType int)  {
	for id := range rf.peers{
		if id != rf.me{
			go rf.syncWithFollower(id, syncType)
		}
	}
}

func (rf *Raft)syncWithFollower(server int, syncType int)  {
	reply := &AppendEntriesReply{}
	for !reply.Success{
		rf.mu.Lock()
		appendArgs := &AppendEntriesArgs{
			Term:         rf.raftState.currentTerm,
			LeadId:       rf.me,
			PrevLogIndex: rf.nextIndex[server]-1,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		if syncType == SyncEntries{
			appendArgs.Entries = rf.raftState.logEntries[rf.nextIndex[server]-1:]
		}
		if appendArgs.PrevLogIndex != 0{
			appendArgs.PrevLogTerm = rf.raftState.logEntries[appendArgs.PrevLogIndex-1].Term
		}else{
			appendArgs.PrevLogTerm = 0
		}
		rf.mu.Unlock()
		replyCh := make(chan *AppendEntriesReply)
		if syncType == SyncEntries{
			fmt.Printf("server %d send entries[%d:] to server %d\n", rf.me, rf.nextIndex[server], server)
		}
		go rf.sendAppendEntries(server, appendArgs, replyCh)
		select {
		case reply = <-replyCh:
			//fmt.Printf("in leader chan returned, reply from server %d: %v\n", server, reply)
		case <-time.After(80*time.Millisecond):
			// if time out, repeat again
			continue
		}
		if reply.Success{
			break
		}
		rf.mu.Lock()
		if rf.nextIndex[server] > 1{
			rf.nextIndex[server]--
		}
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.nextIndex[server] = len(rf.raftState.logEntries)+1
	rf.matchIndex[server] = rf.nextIndex[server] - 1
	rf.mu.Unlock()
	//fmt.Printf("set matchIndex[%d]=%d\n", server, rf.matchIndex[server])
}

func (rf *Raft) updateLeaderCommitIndex()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.raftState.logEntries)==0{
		return
	}
	committed := rf.commitIndex
	min := rf.commitIndex
	max := rf.commitIndex
	fmt.Println("Leader matchIndex:", rf.matchIndex)
	for _, match := range rf.matchIndex{
		if match > max{
			max = match
		}
	}
	if max == committed{
		return
	}
	for min + 1 < max{
		mid := (min + max)>>1
		if isMajorityMatch(rf.matchIndex, mid) && rf.raftState.logEntries[mid-1].Term == rf.raftState.currentTerm{
			min = mid
		}else{
			max = mid
		}
	}
	//fmt.Println("in update commitIndex", max, rf.raftState.logEntries)
	if isMajorityMatch(rf.matchIndex, max) && rf.raftState.logEntries[max-1].Term == rf.raftState.currentTerm{
		rf.commitIndex = max
	}else{
		rf.commitIndex = min
	}
	fmt.Printf("Leader, last commitIndex:%d, new commitIndex:%d\n", committed, rf.commitIndex)
	if committed != rf.commitIndex{
		for index:=committed+1; index<=rf.commitIndex; index++{
			fmt.Printf("server %d commit index:%d\n", rf.me, rf.commitIndex)
			rf.applyCh<-ApplyMsg{
				CommandValid: true,
				Command:      rf.raftState.logEntries[index-1].Command,
				CommandIndex: index,
			}
		}
	}
}

func isMajorityMatch(matchIndex []int, index int) bool {
	count := 1
	for _, match := range matchIndex{
		if match >= index {
			count ++
		}
	}
	return count > len(matchIndex) - count
}
