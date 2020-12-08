package rose

import (
	"fmt"
	"io/ioutil"
	"os"
)

type AppResult struct {
	ID   int
	Method string
	Status string
	Reason string
}

type Rose struct {
	db *db
	isInShutdown bool
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
			fmt.Println("\033[33mwarning:\033[0m", "Defragmenting existing database. DO NOT STOP THIS PROCESS! Depending on the size of the database, this may take some time...")
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
	m := newDb(newFsDriver(dbDir), newFsDriver(dbDir), newFsDriver(dbDir))

	if log {
		fmt.Println("")
		fmt.Println("\033[32minfo:\033[0m " + "Loading indexes...")
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
		fmt.Printf("\033[32m" + "Rose is ready to use!" + "\033[0m" + "\n\n")
	}

	if log {
		fmt.Println("=============")
		fmt.Println("")
	}

	return r, nil
}

func (a *Rose) NewCollection(name string) Error {
	if a.isInShutdown {
		return nil
	}

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

func (a *Rose) Write(m WriteMetadata) (*AppResult, Error) {
	if a.isInShutdown {
		return nil, nil
	}

	if err := validateData(m.Data); err != nil {
		return nil, err
	}

	// save the entry under idx into memory
	_, ID, err := a.db.Write(m.Data)

	if err != nil {
		return nil, err
	}

	return &AppResult{
		ID:   ID,
		Method: WriteMethodType,
		Status: OkResultStatus,
	}, nil
}

func (a *Rose) Read(m ReadMetadata) (*AppResult, Error) {
	if a.isInShutdown {
		return nil, nil
	}

	res := a.db.Read(m.ID, m.Data)

	if res == nil {
		return &AppResult{
			ID: m.ID,
			Method: ReadMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with ID %d not found", m.ID),
		}, nil
	}

	return &AppResult{
		Method: ReadMethodType,
		Status: FoundResultStatus,
	}, nil
}

func (a *Rose) Delete(m DeleteMetadata) (*AppResult, Error) {
	if a.isInShutdown {
		return nil, nil
	}

	res, err := a.db.Delete(m.ID)

	if err != nil {
		return nil, err
	}

	if !res {
		return &AppResult{
			ID: m.ID,
			Method: DeleteMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with ID %d not found", m.ID),
		}, nil
	}

	return &AppResult{
		Method: DeleteMethodType,
		Status: DeletedResultStatus,
	}, nil
}

func (a *Rose) Size() (uint64, Error) {
	if a.isInShutdown {
		return 0, nil
	}

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
	a.isInShutdown = true
	errors := a.db.Shutdown()
	msg := ""

	for _, e := range errors {
		if e != nil {
			msg += e.Error() + "\n"
		}
	}

	if msg != "" {
		base := fmt.Sprintf("Shutdown failed with these errors:\n%s", msg)

		return &systemError{
			Code:    SystemErrorCode,
			Message: base,
		}
	}

	return nil
}
