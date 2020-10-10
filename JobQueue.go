package rose

import (
	"sync"
)

type jobQueue struct {
	Num uint64
	Lock *sync.RWMutex
	FsDbHandler *fsDb
}

type job struct {
	Entry *[]byte
	Index *[]byte
}

func (jb *jobQueue) Add(j *job) {
	jb.FsDbHandler.Write(j.Entry)
}

func (jb *jobQueue) Close() {
	jb.FsDbHandler.SyncAndClose()
}

func newJobQueue() *jobQueue {
	return &jobQueue{
		Num:  0,
		Lock: &sync.RWMutex{},
		FsDbHandler: newFsDbHandler(),
	}
}
