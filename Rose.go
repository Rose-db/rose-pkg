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
	balancer *balancer
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

		m := newDb(newFsDriver(driverDir, writeDriver), newFsDriver(driverDir, updateDriver), newFsDriver(driverDir, updateDriver), collName)

		collections[collName] = m
	}

	return collections, nil
}

func New(output bool) (*Rose, Error) {
	if output {
		fmt.Println("")
		fmt.Println("=============")
		fmt.Println("")
	}

	_, err := createDbIfNotExists(output)

	if err != nil {
		return nil, err
	}

	dbs, err := createDatabases()

	if err != nil {
		return nil, err
	}

	r := &Rose{
		Databases: dbs,
		balancer: newBalancer(),
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

	a.Databases[name] = newDb(newFsDriver(collDir, writeDriver), newFsDriver(collDir, updateDriver), newFsDriver(collDir, updateDriver), name)

	return nil
}

func (a *Rose) Write(m WriteMetadata) (*AppResult, Error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}

	if err := validateData(m.Data); err != nil {
		return nil, err
	}

	db, ok := a.Databases[m.CollectionName]

	if !ok {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Invalid write request. Collection %s does not exist", m.CollectionName),
		}
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
	if err := m.Validate(); err != nil {
		return nil, err
	}

	db, ok := a.Databases[m.CollectionName]

	if !ok {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Invalid read request. Collection %s does not exist", m.CollectionName),
		}
	}

	res, err := db.ReadStrategic(m.ID, m.Data)

	if res == nil && err == nil {
		return &AppResult{
			ID: m.ID,
			Method: ReadMethodType,
			Status: NotFoundResultStatus,
			Reason: fmt.Sprintf("Rose: Entry with ID %d not found", m.ID),
		}, nil
	}

	if err != nil {
		return nil, err
	}

	return &AppResult{
		ID: m.ID,
		Method: ReadMethodType,
		Status: FoundResultStatus,
	}, nil
}

func (a *Rose) Delete(m DeleteMetadata) (*AppResult, Error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}

	db, ok := a.Databases[m.CollectionName]

	if !ok {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Invalid read request. Collection %s does not exist", m.CollectionName),
		}
	}

	res, err := db.Delete(m.ID)

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

func (a *Rose) Replace(m ReplaceMetadata) (*AppResult, Error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}

	if err := validateData(m.Data); err != nil {
		return nil, err
	}


	db, ok := a.Databases[m.CollectionName]

	if !ok {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Invalid read request. Collection %s does not exist", m.CollectionName),
		}
	}

	err := db.Replace(m.ID, m.Data)

	if err != nil {
		return nil, err
	}

	return &AppResult{
		Method: ReplaceMethodType,
		Status: ReplacedResultStatus,
	}, nil
}

func (a *Rose) Query(q *QueryBuilder) ([]*QueryResult, Error) {
	stmt := q.IfStmt
	db, _ := a.Databases[stmt.Equal.Collection]

	ch := make(chan *queueResponse)

	queryItem := &balancerRequest{
		BlockNum: uint16(len(db.BlockTracker)),
		Item: struct {
			CollName string
			Field    string
			Value    interface{}
			DataType dataType
		}{
			CollName: stmt.Equal.Collection,
			Field: stmt.Equal.Field,
			Value: stmt.Equal.Value,
			DataType: stmt.Equal.DataType,
		},
		Response: ch,
	}

	return a.balancer.Push(queryItem), nil
}

func (a *Rose) Size() (uint64, Error) {
	var size uint64
	colls, err := ioutil.ReadDir(roseDbDir())

	if err != nil {
		return 0, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Could not determine size: %s", err.Error()),
		}
	}

	for _, fi := range colls {
		files, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), fi.Name()))

		if err != nil {
			return 0, &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Could not determine size: %s", err.Error()),
			}
		}

		for _, f := range files {
			size += uint64(f.Size())
		}
	}

	return size, nil
}

func (a *Rose) Shutdown() Error {
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

	a.balancer.Close()

	return nil
}
