# mit6.824
This is the lab of MIT 6.824 2018.

1. Lab 1 MapReduce hasn't test
2. Lab 2 Raft:
   Outline of the API that raft expose to
   the service (or tester):
   
   rf = Make(...)
     create a new Raft server.
   rf.Start(command interface{}) (Index, Term, isleader)
     start agreement on a new log entry
   rf.GetState() (Term, isLeader)
     ask a Raft for its current Term, and whether it thinks it is leader
   + 2A: Passed TestElection, TestReElection
   + 2B: Passed TestBasicAgree2B, TestFailAgree2B, TestFailNoAgree2B
