package rose

import "fmt"

type Rose struct {
	memDb *memDb
	jobQueue *jobQueue
}

type AppResult struct {
	Id uint64
	Method string
	Status string
	Reason string
	Result string
}

func (a *Rose) Insert(m *Metadata) (*AppResult, RoseError) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return nil, vErr
	}

	var idx uint64
	var data *[]uint8

	data = &m.Data

	// save the entry under idx into memory
	idx, _ = a.memDb.Insert(m.Id, data)

	// create a copy of the data so that we don't mutate the one
	// in memory
	cp := m.Data
	cpp := &cp

	// create the string to be saved as a single row on fs
	*cpp = append(*cpp, uint8(10))
	b := []uint8(m.Id + " ")
	*cpp = append(b, *cpp...)

	a.jobQueue.Add(&job{
		Entry: cpp,
	})

	return &AppResult{
		Id:     idx,
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
	var err *dbReadError
	res, err = a.memDb.Read(m.Id)

	if err != nil {
		return &AppResult{
			Id:     0,
			Method: ReadMethodType,
			Status: NotFoundResultStatus,
			Reason: err.Error(),
			Result: "",
		}, nil
	}

	return &AppResult{
		Id:     res.Idx,
		Method: ReadMethodType,
		Status: FoundResultStatus,
		Result: res.Result,
	}, nil
}

func (a *Rose) Delete(m *Metadata) (RoseError, *AppResult) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return vErr, nil
	}

	return nil, &AppResult{
		Id:     1,
		Method: DeleteMethodType,
		Status: FoundResultStatus,
	}
}

func (a *Rose) Shutdown() {
	a.jobQueue.Close()
}

func New(log bool) *Rose {
	fileDb := createDbIfNotExists(log)

	m := newMemoryDb()

	if log {
		fmt.Println("Populating existing filesystem database in memory...")
	}

	populateDb(m, fileDb)

	if log {
		fmt.Println("Filesystem database is populated successfully")
	}

	r := &Rose{
		memDb: m,
		jobQueue: newJobQueue(newFsDb(fileDb)),
	}

	return r
}
