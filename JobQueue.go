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

	go func(j *job, open int, limit int) {
		if open >= limit {
			time.Sleep(5 * time.Millisecond)
		}

		fsWrite(j.Id, j.Value)
	}(j, jq.Open, jq.Limit)

	jq.Lock.Lock()
	jq.Open--
	jq.Lock.Unlock()
}
