package rose

import (
	"fmt"
	"io/ioutil"
)

func createDatabases() (map[string]*db, Error) {
	dbDir := roseDbDir()

	stats, err := ioutil.ReadDir(dbDir)

	if err != nil {
		return nil, newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Creating collection databases failed. This is probably a permissons error but you can find out more from the error message: %s", err.Error()))
	}

	collections := make(map[string]*db)

	for _, d := range stats {
		collName := d.Name()
		driverDir := fmt.Sprintf("%s/%s", roseDbDir(), collName)

		files, err := ioutil.ReadDir(driverDir)

		if err != nil {
			return nil, newError(SystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Unable to read collection directory. This is system error and Rose cannot boot: %s", err.Error()))
		}

		var blocksNum uint16
		for _, f := range files {
			if !f.IsDir() {
				blocksNum++
			}
		}

		w, dErr := newFsDriver(driverDir, writeDriver)

		if dErr != nil {
			return nil, dErr
		}

		r, dErr := newFsDriver(driverDir, updateDriver)

		if dErr != nil {
			return nil, dErr
		}

		d, dErr := newFsDriver(driverDir, updateDriver)

		if dErr != nil {
			return nil, dErr
		}

		m := newDb(
			w,
			r,
			d,
			collName,
			blocksNum,
		)

		collections[collName] = m
	}

	return collections, nil
}

func boot(output bool) (*Rose, Error) {
	if output {
		fmt.Println("")
		fmt.Println("=============")
		fmt.Println("")
	}

	_, err := createDbIfNotExists(output)

	if err != nil {
		return nil, err
	}

	err = createIndexLocationIfNotExists()

	if err != nil {
		return nil, err
	}

	dbs, err := createDatabases()

	if err != nil {
		return nil, err
	}

	fsIdx, err := newIndexHandler()

	if err != nil {
		return nil, err
	}

	r := &Rose{
		Databases: dbs,
		fsIndexHandler: fsIdx,
	}

	if err := loadIndexes(r.Databases, output); err != nil {
		return nil, err
	}

	if output {
		fmt.Println("=============")
		fmt.Println("")
	}

	return r, nil
}