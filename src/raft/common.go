package raft

import (
	"math/rand"
	"time"
)

type NodeState int

const (
	LEADER NodeState = iota
	FOLLOWER
	CANDIDATER
)

const HeartbeatTime = time.Duration(100) * time.Millisecond

// [500, 650)
func randElectionTime() time.Duration {
	ms := 500 + (rand.Int63() % 150)
	return time.Duration(ms) * time.Millisecond
}
