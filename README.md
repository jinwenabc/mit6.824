# mit6.824
This is the lab of MIT 6.824 2018.

1. Lab 1 MapReduce hasn't test
2. Lab 2 Raft:  
   Set DebugMode to true while debuging to print debug logs.   
   Outline of the API that raft expose to
   the service (or tester):  
   1. rf = Make(...)  
   create a new Raft server.  
   2. rf.Start(command interface{}) (Index, Term, isleader)  
   start agreement on a new log entry  
   3. rf.GetState() (Term, isLeader)  
   ask a Raft for its current Term, and whether it thinks it is leader
   
  + 2A: Passed TestElection, TestReElection
  + 2B: Passed TestBasicAgree2B, TestFailAgree2B, TestFailNoAgree2B, TestConcurrentStarts2B, TestRejoin2B, TestBackup2B, TestCount2B
  + 2C: Passed TestPersist12C, TestPersist22C, TestPersist32C, TestFigure82C, TestUnreliableAgree2C, TestFigure8Unreliable2C, TestReliableChurn2C, TestUnreliableChurn2C
