package rose

import (
	"fmt"
	"io/ioutil"
)

type Rose struct {
	db *Db
}

type AppResult struct {
	Method string
	Status string
	Reason string
}

func New(log bool) (*Rose, Error) {
	comm := make(chan string)
	errChan := make(chan Error)
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
		fmt.Println("Loading existing filesystem database in memory. Depending on the size of the database, this may take some time...")
	}

	err = loadDbInMemory(m, log)

	if err != nil {
		return nil, err
	}

	if log {
		fmt.Println("Filesystem database is loaded successfully")
	}

	r := &Rose{
		db: m,
	}

	return r, nil
}

func (a *Rose) Write(m *Metadata) (*AppResult, Error) {
	vErr := m.validate()

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

func (a *Rose) Read(id string, v interface{}) (*AppResult, Error) {
	if id == "" {
		return nil, &metadataError{
			Code:    MetadataErrorCode,
			Message: "Id cannot be an empty string",
		}
	}

	res := a.db.Read(id, v)

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
	}, nil
}

func (a *Rose) Delete(id string) (*AppResult, Error) {
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

	if !res {
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

func (a *Rose) Size() (uint64, Error) {
	files, err := ioutil.ReadDir(roseDbDir())

	if err != nil {
		return 0, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Could not determine size: %s", err.Error()),
		}
	}

	var size uint64

	for _, f := range files {
		size += uint64(f.Size())
	}

	return size, nil
}

func (a *Rose) Shutdown() Error {
	return a.db.Shutdown()
}
