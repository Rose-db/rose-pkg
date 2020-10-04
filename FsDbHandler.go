package rose

import (
	"fmt"
	"os"
)

type fsDbHandler struct {
	File *os.File
}

func newFsDbHandler() *fsDbHandler {
	a := &fsDbHandler{}

	a.Init()

	return a
}

func (fs *fsDbHandler) Init() uint8 {
	if fs.File == nil {
		fs.File = fs.createFile()

		return 0
	}

	return 1
}

func (fs *fsDbHandler) Write(d *[]byte) {
	var err error
	var name string

	name = fs.File.Name()

	_, err = fs.File.Write(*d)

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot write to existing file %s with underlying message: %s", name, err.Error()),
		})
	}
}

func (fs *fsDbHandler) createFile() *os.File {
	var f string
	var file *os.File

	f = fmt.Sprintf("%s/db/%s", roseDir(), "rose.rose")

	file, err := os.Create(f)

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot create file %s with underlying message: %s", f, err.Error()),
		})
	}

	return file
}

func (fs *fsDbHandler) syncAndClose() {
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

