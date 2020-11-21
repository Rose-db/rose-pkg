package rose

import (
	"fmt"
	"io/ioutil"
)

type Rose struct {
	db *Db
}

type AppResult struct {
	Uuid string
	Method string
	Status string
	Reason string
}

type GoAppResult struct {
	Result *AppResult
	Err Error
}

func New(log bool) (*Rose, Error) {
	if log {
		fmt.Println("=============")
		fmt.Println("")
	}

	created, err := createDbIfNotExists(log)

	if err != nil {
		return nil, err
	}

	if !created {
		if log {
			fmt.Println(string("\033[33mWARNING:\033[0m"),"Defragmenting existing database. DO NOT STOP THIS PROCESS! Depending on the size of the database, this may take some time...")
		}

		if err := defragment(log); err != nil {
			return nil, err
		}

		if log {
			fmt.Println("  Defragmentation complete!")
			fmt.Println("")
		}
	}

	m := newMemoryDb(newFsDriver(roseDbDir()))

	if log {
		fmt.Println("=============")
		fmt.Println("")
		fmt.Printf(string("\033[32mINFO: \033[0m") + "Loading existing filesystem database in memory. Depending on the size of the database, this may take some time...\n\n")
	}

	if err := loadDbInMemory(m, log); err != nil {
		return nil, err
	}

	if log {
		fmt.Println("")
		fmt.Printf(string("\033[32mINFO: \033[0m") + "Filesystem database is loaded successfully!\n\n")
		fmt.Println("=============")
		fmt.Println("")
	}

	r := &Rose{
		db: m,
	}

	if log {
		fmt.Printf(string("\033[32mINFO: ") + "Rose is ready to use!" + string("\033[0m") + "\n\n")
	}

	return r, nil
}

func (a *Rose) Write(data []uint8) (*AppResult, Error) {
	if err := validateData(data); err != nil {
		return nil, err
	}

	// save the entry under idx into memory
	_, id, err := a.db.Write(data, true)

	if err != nil {
		return nil, err
	}

	return &AppResult{
		Uuid: id,
		Method: InsertMethodType,
		Status: OkResultStatus,
	}, nil
}

func (a *Rose) GoWrite(data []uint8) chan *GoAppResult {
	resChan := make(chan *GoAppResult)

	if err := validateData(data); err != nil {
		resChan<- &GoAppResult{
			Result: nil,
			Err:    err,
		}
	}

	// save the entry under idx into memory
	go a.db.GoWrite(data, true, resChan)

	return resChan
}

func (a *Rose) Read(id string, v interface{}) (*AppResult, Error) {
	if id == "" {
		return nil, &dataError{
			Code:    DataErrorCode,
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
		return nil, &dataError{
			Code:    DataErrorCode,
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

func (a *Rose) GoDelete(id string) chan *GoAppResult {
	resChan := make(chan *GoAppResult)
	if id == "" {
		resChan<- &GoAppResult{
			Result: nil,
			Err:    &dataError{
				Code:    DataErrorCode,
				Message: "Id cannot be an empty string",
			},
		}

		return resChan
	}

	go a.db.GoDelete(id, resChan)

	return resChan
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
