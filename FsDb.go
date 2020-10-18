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

func (fs *fsDb) Write(d *[]uint8) {
	var err error

	_, err = fs.File.Write(*d)

	if err != nil {
		name := fs.File.Name()

		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot write to existing file %s with underlying message: %s", name, err.Error()),
		})
	}
}

func (fs *fsDb) Delete(uint82 *[]uint8) {

}

func (fs *fsDb) open(n string) *os.File {
	var f string
	var file *os.File

	f = fmt.Sprintf("%s/db/%s", roseDir(), n)

	file, err := os.OpenFile(f, os.O_RDWR, os.ModeAppend)

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot create file %s with underlying message: %s", f, err.Error()),
		})
	}

	return file
}

func (fs *fsDb) SyncAndClose() {
	var err error
	var name string

	name = fs.File.Name()
	err = fs.File.Sync()

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Database file system problem for file %s with underlying message: %s", name, err.Error()),
		})
	}

	err = fs.File.Close()

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot close file %s with underlying message: %s", name, err.Error()),
		})
	}
}

