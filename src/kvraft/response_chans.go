package kvraft

import "sync"

type ResponseChans struct {
	mu    sync.Mutex
	chans map[int]chan OperateReply
}

func NewResponseChans() ResponseChans {
	return ResponseChans{
		mu:    sync.Mutex{},
		chans: make(map[int]chan OperateReply),
	}
}

func (rc *ResponseChans) getResponseChan(index int) chan OperateReply {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	_, ok := rc.chans[index]
	if !ok {
		rc.chans[index] = make(chan OperateReply, 1)
	}
	return rc.chans[index]
}

// func (rc *ResponseChans) freeChan(index int) {
// 	if _, ok := rc.chans[index]; !ok {
// 		return
// 	}

// 	close(rc.chans[index])
// 	delete(rc.chans, index)
// }
