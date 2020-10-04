package rose

import (
	"fmt"
)

type AppController struct {
	Database *database
	FsJobQueue *fsJobQueue
}

type AppResult struct {
	Id uint
	Method string
	Status string
	Reason string
	Result string
}

func (a *AppController) Run(m *Metadata) (RoseError, *AppResult) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return vErr, nil
	}

	if m.Method == InsertMethodType {
		return a.insert(m)
	} else if m.Method == DeleteMethodType {
		return a.delete(m)
	} else if m.Method == ReadMethodType {
		return a.read(m)
	}

	panic("Internal rose error. Unreachable code reached. None of the methods have executed but one should have.")
}

func (a *AppController) insert(m *Metadata) (RoseError, *AppResult) {
	var idx uint

	idx = a.Database.Insert(m.Id, m.Data)

	go a.FsJobQueue.Run(&job{
		Id:    idx,
		Value: m.Data,
	})

	return nil, &AppResult{
		Id:     idx,
		Method: m.Method,
		Status: FoundResultStatus,
	}
}

func (a *AppController) read(m *Metadata) (RoseError, *AppResult) {
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

func (a *AppController) delete(m *Metadata) (RoseError, *AppResult) {
	return nil, &AppResult{
		Id:     1,
		Method: m.Method,
		Status: FoundResultStatus,
	}
}

func (a *AppController) Init(log bool) chan RoseError {
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

	a.Database = NewDatabase()

	a.FsJobQueue = newJobQueue(200)

	return errStream
}
