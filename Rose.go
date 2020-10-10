package rose

type Rose struct {
	Database *database
	JobQueue *jobQueue
}

type AppResult struct {
	Id uint64
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

	var idx uint64
	var data *[]uint8

	data = &m.Data

	// save the entry under idx into memory
	idx, _ = a.Database.Insert(m.Id, data)

	// create a copy of the data so that we don't mutate the one
	// in memory
	cp := m.Data
	cpp := &cp

	// create the string to be saved as a single row on fs
	*cpp = append(*cpp, byte(10))
	b := []uint8(m.Id + " ")
	*cpp = append(b, *cpp...)

	a.JobQueue.Add(&job{
		Entry: cpp,
	})

	return nil, &AppResult{
		Id:     idx,
		Method: InsertMethodType,
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
			Method: ReadMethodType,
			Status: NotFoundResultStatus,
			Reason: err.Error(),
			Result: "",
		}
	}

	return nil, &AppResult{
		Id:     res.Idx,
		Method: ReadMethodType,
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
		Method: DeleteMethodType,
		Status: FoundResultStatus,
	}
}

func New(log bool) *Rose {
	createDbIfNotExists(log)

	r := &Rose{
		Database: newDatabase(),
		JobQueue: newJobQueue(),
	}

	return r
}
