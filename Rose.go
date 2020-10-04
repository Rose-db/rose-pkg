package rose

import (
	"fmt"
)


type Rose struct {
	Database *database
	FsDbHandler *fsDbHandler
}

type AppResult struct {
	Id uint
	Method string
	Status string
	Reason string
	Result string
}

func (a *Rose) Insert(m *Metadata) (RoseError, *AppResult) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return vErr, nil
	}

	var idx, blockIdx uint

	idx, blockIdx = a.Database.Insert(m.Id, m.Data)

	a.FsDbHandler.AcquireBlock(blockIdx)
	a.FsDbHandler.Write(idx, m.Data)

	return nil, &AppResult{
		Id:     idx,
		Method: m.Method,
		Status: FoundResultStatus,
	}
}

func (a *Rose) Read(m *Metadata) (RoseError, *AppResult) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return vErr, nil
	}

	var res *dbReadResult
	var err *dbReadError
	res, err = a.Database.Read(m.Id)

	if err != nil {
		return nil, &AppResult{
			Id:     0,
			Method: m.Method,
			Status: NotFoundResultStatus,
			Reason: err.Error(),
			Result: "",
		}
	}

	return nil, &AppResult{
		Id:     res.Idx,
		Method: m.Method,
		Status: FoundResultStatus,
		Result: res.Result,
	}
}

func (a *Rose) Delete(m *Metadata) (RoseError, *AppResult) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return vErr, nil
	}

	return nil, &AppResult{
		Id:     1,
		Method: m.Method,
		Status: FoundResultStatus,
	}
}

func (a *Rose) Init(log bool) chan RoseError {
	var fsStream chan string
	var errStream chan RoseError

	fsStream = make(chan string)
	errStream = make(chan RoseError)

	go createDbIfNotExists(fsStream, errStream)

	for msg := range fsStream {
		if log {
			fmt.Println(msg)
		}
	}

	a.Database = newDatabase()
	a.FsDbHandler = newFsDbHandler()

	return errStream
}
