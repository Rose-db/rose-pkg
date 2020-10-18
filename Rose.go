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

func New(log bool) *Rose {
	file := createDbIfNotExists(log)

	m := newMemoryDb()

	if log {
		fmt.Println("Populating existing filesystem database in memory...")
	}

	populateDb(m, file)

	if log {
		fmt.Println("Filesystem database is populated successfully")
	}

	r := &Rose{
		memDb: m,
		jobQueue: newJobQueue(newFsDb(file)),
	}

	return r
}

func (a *Rose) Insert(m *Metadata) (*AppResult, RoseError) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return nil, vErr
	}

	var data *[]uint8

	data = &m.Data

	// save the entry under idx into memory
	res := a.memDb.Insert(m.Id, data)

	if res == false {
		return &AppResult{
			Method: InsertMethodType,
			Status: DuplicatedIdStatus,
		}, nil
	}

	// create a copy of the data so that we don't mutate the one
	// in memory
	cp := m.Data
	cpp := &cp

	// create the string to be saved as a single row on fs
	*cpp = append(*cpp, uint8(10))
	b := []uint8(m.Id + " ")
	*cpp = append(b, *cpp...)

	a.jobQueue.AddSync(&job{
		Entry: cpp,
	})

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

func (a *Rose) Shutdown() {
	a.jobQueue.Close()
}
