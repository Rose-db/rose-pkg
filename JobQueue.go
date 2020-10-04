package rose

import (
	"sync"
	"time"
)

type fsJobQueue struct {
	Limit int
	Open int
	Lock *sync.RWMutex
}

type job struct {
	Id uint
	Value *[]byte
}

func newJobQueue(limit int) *fsJobQueue {
	return &fsJobQueue{
		Limit: limit,
		Lock: &sync.RWMutex{},
		Open: 0,
	}
}

func (jq *fsJobQueue) Run(j *job) {
	jq.Lock.Lock()
	jq.Open++
	jq.Lock.Unlock()

	if jq.Open >= jq.Limit {
		time.Sleep(2000 * time.Millisecond)
	}

	fsWrite(j.Id, j.Value)

	jq.Lock.Lock()
	jq.Open--
	jq.Lock.Unlock()
}
