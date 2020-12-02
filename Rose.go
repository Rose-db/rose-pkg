package rose

import (
	"fmt"
	"io/ioutil"
	"os"
)

type Rose struct {
	db *Db
}

type AppResult struct {
	ID   int
	Method string
	Status string
	Reason string
}

type GoAppResult struct {
	Result *AppResult
	Err    Error
}

func New(doDefragmentation bool, log bool) (*Rose, Error) {
	if log {
		fmt.Println("")
		fmt.Println("=============")
		fmt.Println("")
	}

	_, err := createDbIfNotExists(log)

	if err != nil {
		return nil, err
	}

	if doDefragmentation {
		if log {
			fmt.Println(string("\033[33mwarning:\033[0m"), "Defragmenting existing database. DO NOT STOP THIS PROCESS! Depending on the size of the database, this may take some time...")
		}

		if err := defragment(log); err != nil {
			return nil, err
		}

		if log {
			fmt.Println("  Defragmentation complete!")
			fmt.Println("")
		}
	}

	dbDir := roseDbDir()
	m := newMemoryDb(newFsDriver(dbDir), newFsDriver(dbDir), newFsDriver(dbDir))

	if log {
		fmt.Println("")
		fmt.Println(string("\033[32minfo:\033[0m ") + "Loading indexes...")
	}

	if err := loadIndexes(m, log); err != nil {
		return nil, err
	}

	if log {
		fmt.Println("      Indexes loaded")
		fmt.Println("")
	}

	r := &Rose{
		db: m,
	}

	if log {
		fmt.Printf(string("\033[32m") + "Rose is ready to use!" + string("\033[0m") + "\n\n")
	}

	if log {
		fmt.Println("=============")
		fmt.Println("")
	}

	return r, nil
}

func (a *Rose) NewCollection(name string) Error {
	collDir := fmt.Sprintf("%s/%s", roseDbDir(), name)

	_, err := os.Stat(collDir)

	if err == nil {
		return nil
	}

	if err := os.Mkdir(collDir, 0666); err != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Unable to create collection directory with underlying error: %s", err.Error()),
		}
	}

	return nil
}

func (a *Rose) Write(data []uint8) (*AppResult, Error) {
	if err := validateData(data); err != nil {
		return nil, err
	}

	// save the entry under idx into memory
	_, ID, err := a.db.Write(data, true)

	if err != nil {
		return nil, err
	}

	return &AppResult{
		ID:   ID,
		Method: WriteMethodType,
		Status: OkResultStatus,
	}, nil
}

func (a *Rose) GoWrite(data []uint8) chan *GoAppResult {
	resChan := make(chan *GoAppResult)

	if err := validateData(data); err != nil {
		resChan <- &GoAppResult{
			Result: nil,
			Err:    err,
		}
	}

	// save the entry under idx into memory
	go a.db.GoWrite(data, true, resChan)

	return resChan
}

func (a *Rose) Read(ID int, data interface{}) (*AppResult, Error) {
	res := a.db.Read(ID, data)

	if res == nil {
		return &AppResult{
			ID: ID,
			Method: ReadMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with ID %d not found", ID),
		}, nil
	}

	return &AppResult{
		Method: ReadMethodType,
		Status: FoundResultStatus,
	}, nil
}

func (a *Rose) Delete(ID int) (*AppResult, Error) {
	res, err := a.db.Delete(ID)

	if err != nil {
		return nil, err
	}

	if !res {
		return &AppResult{
			ID: ID,
			Method: DeleteMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with ID %d not found", ID),
		}, nil
	}

	return &AppResult{
		Method: DeleteMethodType,
		Status: DeletedResultStatus,
	}, nil
}

func (a *Rose) GoDelete(ID int) chan *GoAppResult {
	resChan := make(chan *GoAppResult)

	go a.db.GoDelete(ID, resChan)

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
