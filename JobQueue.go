package rose

import (
	"sync"
)

type jobQueue struct {
	Num uint64
	Lock *sync.RWMutex
	FsDbHandler *fsDbHandler
}

type job struct {
	Entry *[]byte
	Index *[]byte
}

func (jb *jobQueue) Add(j *job) {
	jb.FsDbHandler.Write(j.Entry)
}

func newJobQueue() *jobQueue {
	return &jobQueue{
		Num:  0,
		Lock: &sync.RWMutex{},
		FsDbHandler: newFsDbHandler(),
	}
}
