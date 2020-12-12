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
	Databases map[string]*db
	db *db
	isInShutdown bool
}

var createDatabases = func() (map[string]*db, Error) {
	dbDir := roseDbDir()

	stats, err := ioutil.ReadDir(dbDir)

	if err != nil {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Creating collection databases failed: %s", err.Error()),
		}
	}

	collections := make(map[string]*db)

	for _, d := range stats {
		collName := d.Name()
		driverDir := fmt.Sprintf("%s/%s", roseDbDir(), collName)

		m := newDb(newFsDriver(driverDir), newFsDriver(driverDir), newFsDriver(driverDir))

		collections[collName] = m
	}

	return collections, nil
}

func New(doDefragmentation bool, output bool) (*Rose, Error) {
	if output {
		fmt.Println("")
		fmt.Println("=============")
		fmt.Println("")
	}

	_, err := createDbIfNotExists(output)

	if err != nil {
		return nil, err
	}

	if doDefragmentation {
		if output {
			fmt.Println("\033[33mwarning:\033[0m", "Defragmenting existing database. DO NOT STOP THIS PROCESS! Depending on the size of the database, this may take some time...")
		}

		if err := defragment(output); err != nil {
			return nil, err
		}

		if output {
			fmt.Println("  Defragmentation complete!")
			fmt.Println("")
		}
	}

	dbs, err := createDatabases()

	if err != nil {
		return nil, err
	}

	r := &Rose{
		Databases: dbs,
	}

	if output {
		fmt.Println("")
		fmt.Println("\033[32minfo:\033[0m " + "Loading indexes...")
	}

	if err := loadIndexes(r.Databases); err != nil {
		return nil, err
	}

	if output {
		fmt.Println("      Indexes loaded")
		fmt.Println("")
	}

	if output {
		fmt.Printf("\033[32m" + "Rose is ready to use!" + "\033[0m" + "\n\n")
	}

	if output {
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

	if err := os.Mkdir(collDir, 0755); err != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Unable to create collection directory with underlying error: %s", err.Error()),
		}
	}

	firstBlock := roseBlockFile(0, collDir)
	file, e := createFile(firstBlock, os.O_RDWR|os.O_CREATE)

	if e != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("      Trying to create initial block file failed with underlying message: %s", e.Error()),
		}
	}

	e = closeFile(file)

	if e != nil {
		return e
	}

	a.Databases[name] = newDb(newFsDriver(collDir), newFsDriver(collDir), newFsDriver(collDir))

	return nil
}

func (a *Rose) Write(m WriteMetadata) (*AppResult, Error) {
	if a.isInShutdown {
		return nil, nil
	}

	db, ok := a.Databases[m.CollectionName]

	if !ok {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Invalid write request. Collection %s does not exist", m.CollectionName),
		}
	}

	if err := validateData(m.Data); err != nil {
		return nil, err
	}

	// save the entry under idx into memory
	_, ID, err := db.Write(m.Data)

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

	db, ok := a.Databases[m.CollectionName]

	if !ok {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Invalid write request. Collection %s does not exist", m.CollectionName),
		}
	}

	res := db.Read(m.ID, m.Data)

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

	for _, db := range a.Databases {
		errors := db.Shutdown()
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
	}

	return nil
}
