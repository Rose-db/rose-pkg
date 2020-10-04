package rose

import "sync"

type jobQueue struct {
	Num uint64
	Lock *sync.RWMutex
	FsDbHandler *fsDbHandler
}

type job struct {
	Data *[]byte
}

func (jb *jobQueue) Add(j *job) {
	jb.FsDbHandler.Write(j.Data)
}

func (jb *jobQueue) Close() {
	for {
		if jb.Num <= 0 {
			return
		}
	}
}

func newJobQueue() *jobQueue {
	return &jobQueue{
		Num:  0,
		Lock: &sync.RWMutex{},
		FsDbHandler: newFsDbHandler(),
	}
}
