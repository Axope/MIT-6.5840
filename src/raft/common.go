package raft

import (
	"math/rand"
	"time"
)

const (
	LEADER = iota
	FOLLOWER
	CANDIDATER
)

const HeartbeatTime = time.Duration(50) * time.Millisecond

// [300, 450)
func randElectionTime() time.Duration {
	ms := 300 + (rand.Int63() % 150)
	return time.Duration(ms) * time.Millisecond
}
