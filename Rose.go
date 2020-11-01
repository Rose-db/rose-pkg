package rose

import (
	"fmt"
)

type Rose struct {
	memDb *memDb
	jobQueue *jobQueue
}

type AppResult struct {
	Method string
	Status string
	Reason string
	Result string
}

func New(log bool) (*Rose, RoseError) {
	file, err := createDbIfNotExists(log)

	if err != nil {
		return nil, err
	}

	m := newMemoryDb()

	if log {
		fmt.Println("Populating existing filesystem database in memory...")
	}

	err = populateDb(m, file)

	if err != nil {
		return nil, err
	}

	if log {
		fmt.Println("Filesystem database is populated successfully")
	}

	r := &Rose{
		memDb: m,
		jobQueue: newJobQueue(newFsDb(file)),
	}

	return r, nil
}

func (a *Rose) Write(m *Metadata) (*AppResult, RoseError) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return nil, vErr
	}

	var data *[]uint8

	data = &m.Data

	// save the entry under idx into memory

	status := a.memDb.Write(m.Id, data)

	if status == NotExistsStatus {
		return &AppResult{
			Method: InsertMethodType,
			Status: DuplicatedIdStatus,
		}, nil
	}

	err := a.jobQueue.AddSync(&job{
		Entry: prepareData(m.Id, m.Data),
	})

	if err != nil {
		return nil, err
	}

	return &AppResult{
		Method: InsertMethodType,
		Status: OkResultStatus,
	}, nil
}

func (a *Rose) Read(m *Metadata) (*AppResult, RoseError) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return nil, vErr
	}

	var res *dbReadResult

	res = a.memDb.Read(m.Id)

	if res == nil {
		return &AppResult{
			Method: ReadMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with id %s not found", m.Id),
		}, nil
	}

	return &AppResult{
		Method: ReadMethodType,
		Status: FoundResultStatus,
		Result: res.Result,
	}, nil
}

func (a *Rose) Delete(m *Metadata) (*AppResult, RoseError) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return nil, vErr
	}

	res := a.memDb.Delete(m.Id)
	e := []uint8(m.Id)
	a.jobQueue.DeleteSync(&job{
		Entry: &e,
	})

	if res == false {
		return &AppResult{
			Method: DeleteMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with id %s not found", m.Id),
		}, nil
	}

	return &AppResult{
		Method: DeleteMethodType,
		Status: EntryDeletedStatus,
	}, nil
}

func (a *Rose) Shutdown() RoseError {
	return a.jobQueue.Close()
}
