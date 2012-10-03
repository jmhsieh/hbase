#!/bin/bash 
mvn test -Dtest=TestTwoPhase*,TestCommitCo*,TestDistributedError*,TestDistributedThree*,testRemoteException*,TestZooKeeperDist* -PlocalTests
