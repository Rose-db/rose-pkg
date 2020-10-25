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
	Entry *[]uint8
	Index *[]uint8
}

func (jb *jobQueue) AddSync(j *job) RoseError {
	return jb.FsDbHandler.Write(j.Entry)
}

func (jb *jobQueue) DeleteSync(j *job) {
	jb.FsDbHandler.Delete(j.Entry)
}

func (jb *jobQueue) Close() {
	jb.FsDbHandler.SyncAndClose()
}

func newJobQueue(fsDb *fsDb) *jobQueue {
	return &jobQueue{
		Num:  0,
		Lock: &sync.RWMutex{},
		FsDbHandler: fsDb,
	}
}
