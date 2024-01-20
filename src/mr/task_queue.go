package mr

import (
	"container/list"
	"errors"
	"sync"
)

type TaskQueue struct {
	list   *list.List
	locker sync.Mutex
}

func NewQueue() *TaskQueue {
	return &TaskQueue{
		list:   list.New(),
		locker: sync.Mutex{},
	}
}

func (q *TaskQueue) Empty() bool {
	q.locker.Lock()
	defer q.locker.Unlock()

	return q.list.Len() == 0
}

func (q *TaskQueue) Size() int {
	q.locker.Lock()
	defer q.locker.Unlock()

	return q.list.Len()
}

func (q *TaskQueue) Push(v Task) {
	q.locker.Lock()
	defer q.locker.Unlock()

	q.list.PushBack(v)
}

func (q *TaskQueue) GetFrontAndPop() (Task, error) {
	q.locker.Lock()
	defer q.locker.Unlock()

	if q.list.Len() == 0 {
		return Task{}, errors.New("queue empty")
	}

	ele := q.list.Front()
	v := ele.Value.(Task)
	q.list.Remove(ele)
	return v, nil
}
