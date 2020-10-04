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

	idx, _ = a.Database.Insert(m.Id, data)

	a.JobQueue.Add(&job{Data: data})

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

func (a *Rose) Close() {
	a.JobQueue.Close()
}

func New() *Rose {
	createDbIfNotExists()

	a := &Rose{
		Database: newDatabase(),
		JobQueue: newJobQueue(),
	}

	return a
}
