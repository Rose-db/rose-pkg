package rose

import (
	"fmt"
	"os"
)

type fsDb struct {
	File *os.File
}

func newFsDb(fileDb *os.File) *fsDb {
	a := &fsDb{
		File: fileDb,
	}

	return a
}

func (fs *fsDb) Write(d *[]uint8) RoseError {
	var err error

	_, err = fs.File.Write(*d)

	if err != nil {
		name := fs.File.Name()

		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot write to existing file %s with underlying message: %s", name, err.Error()),
		}
	}

	return nil
}

func (fs *fsDb) Delete(id *[]uint8) {

}

func (fs *fsDb) SyncAndClose() RoseError {
	var err error
	var name string

	name = fs.File.Name()
	err = fs.File.Sync()

	if err != nil {
		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Database file system problem for file %s with underlying message: %s", name, err.Error()),
		}
	}

	err = fs.File.Close()

	if err != nil {
		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot close file %s with underlying message: %s", name, err.Error()),
		}
	}

	return nil
}

