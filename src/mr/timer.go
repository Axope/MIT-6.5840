package mr

import (
	"fmt"
	"sync"
	"time"
)

type ClientInfo struct {
	ID    string
	Timer *time.Timer
}

type Timer struct {
	clients map[string]*ClientInfo
	mu      sync.Mutex
	timeout int
}

func NewTimer(timeout int) *Timer {
	return &Timer{
		clients: make(map[string]*ClientInfo),
		mu:      sync.Mutex{},
		timeout: timeout,
	}
}

// func (t *Timer) HandleClient(clientID string) {
// 	client := &ClientInfo{
// 		ID:    clientID,
// 		Timer: &time.Timer{},
// 	}
// 	t.mu.Lock()
// 	t.clients[clientID] = client
// 	t.mu.Unlock()

// 	// 启动定时器
// 	client.Timer = time.AfterFunc(time.Second*time.Duration(t.timeout), func() {
// 		t.handleClientTimeout(clientID)
// 	})
// }

// func (t *Timer) handleClientTimeout(clientID string) {
// 	t.mu.Lock()
// 	_, ok := t.clients[clientID]
// 	t.mu.Unlock()

// 	if ok {
// 		t.mu.Lock()
// 		delete(t.clients, clientID)
// 		t.mu.Unlock()
// 	}
// }

func (t *Timer) HandleClient(clientUUID string, task Task, f func(task Task)) {
	client := &ClientInfo{
		ID:    clientUUID,
		Timer: &time.Timer{},
	}
	t.mu.Lock()
	t.clients[clientUUID] = client
	t.mu.Unlock()

	// 启动定时器
	client.Timer = time.AfterFunc(time.Second*time.Duration(t.timeout), func() {
		t.mu.Lock()
		_, ok := t.clients[clientUUID]
		t.mu.Unlock()
		if ok {
			t.mu.Lock()
			delete(t.clients, clientUUID)
			t.mu.Unlock()
		}
		f(task)
	})
}

func (t *Timer) ResetTimer(clientID string) error {
	t.mu.Lock()
	client, ok := t.clients[clientID]
	t.mu.Unlock()

	if ok {
		client.Timer.Reset(time.Second * time.Duration(t.timeout))
		return nil
	}
	return fmt.Errorf("client(%v) not found", clientID)
}
