package rose

import (
	"fmt"
)

type Rose struct {
	db *Db
}

type AppResult struct {
	Method string
	Status string
	Reason string
	Result string
}

func New(log bool) (*Rose, RoseError) {
	comm := make(chan string)
	errChan := make(chan RoseError)
	go createDbIfNotExists(log, comm, errChan)

	for msg := range comm {
		fmt.Println(msg)
	}

	err := <-errChan

	if err != nil {
		return nil, err
	}


	m := newMemoryDb(newFsDriver())

	if log {
		fmt.Println("Populating existing filesystem database in memory...")
	}

	err = populateDb(m)

	if err != nil {
		return nil, err
	}

	if log {
		fmt.Println("Filesystem database is populated successfully")
	}

	r := &Rose{
		db: m,
	}

	return r, nil
}

func (a *Rose) Write(m *Metadata) (*AppResult, RoseError) {
	var vErr RoseError

	vErr = m.validate()

	if vErr != nil {
		return nil, vErr
	}

	// save the entry under idx into memory

	status, err := a.db.Write(m.Id, m.Data, true)

	if err != nil {
		return nil, err
	}

	if status == ExistsStatus {
		return &AppResult{
			Method: InsertMethodType,
			Status: DuplicatedIdStatus,
		}, nil
	}

	return &AppResult{
		Method: InsertMethodType,
		Status: OkResultStatus,
	}, nil
}

func (a *Rose) Read(id string) (*AppResult, RoseError) {
	if id == "" {
		return nil, &metadataError{
			Code:    MetadataErrorCode,
			Message: "Id cannot be an empty string",
		}
	}

	var res *dbReadResult

	res = a.db.Read(id)

	if res == nil {
		return &AppResult{
			Method: ReadMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with id %s not found", id),
		}, nil
	}

	return &AppResult{
		Method: ReadMethodType,
		Status: FoundResultStatus,
		Result: res.Result,
	}, nil
}

func (a *Rose) Delete(id string) (*AppResult, RoseError) {
	if id == "" {
		return nil, &metadataError{
			Code:    MetadataErrorCode,
			Message: "Id cannot be an empty string",
		}
	}

	res, err := a.db.Delete(id)

	if err != nil {
		return nil, err
	}

	if res == false {
		return &AppResult{
			Method: DeleteMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with id %s not found", id),
		}, nil
	}

	return &AppResult{
		Method: DeleteMethodType,
		Status: EntryDeletedStatus,
	}, nil
}

func (a *Rose) Shutdown() RoseError {
	return a.db.Shutdown()
}
