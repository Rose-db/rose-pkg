package rose

import (
	"fmt"
	"os"
)

type fsDb struct {
	Path string
	File *os.File
}

func newFsDb(b uint16) (*fsDb, RoseError) {
	a := roseBlockFile(b)

	file, err := createFile(a, os.O_RDWR|os.O_CREATE)

	if err != nil {
		return nil, err
	}

	return &fsDb{
		File: file,
		Path: a,
	}, nil
}

func (fs *fsDb) Write(d *[]uint8) RoseError {
	if fs.File == nil {
		err := fs.WakeUp()

		if err != nil {
			return err
		}
	}

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

func (fs *fsDb) Sleep() RoseError {
	if err := fs.SyncAndClose(); err != nil {
		return err
	}

	fs.File = nil

	return nil
}

func (fs *fsDb) WakeUp() RoseError {
	file, err := createFile(fs.Path, os.O_RDWR)

	if err != nil {
		return err
	}

	fs.File = file

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

