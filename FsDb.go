package rose

import (
	"fmt"
	"os"
)

type fsDb struct {
	Path string
	File *os.File
	Size int64
}

func newFsDb(b uint16, dbDir string) (*fsDb, Error) {
	a := roseBlockFile(b, dbDir)

	file, err := createFile(a, os.O_RDWR|os.O_CREATE)

	if err != nil {
		return nil, err
	}

	stat, statErr := os.Stat(a)

	if statErr != nil {
		return nil, &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot read stats on existing file %s with underlying message: %s", a, statErr.Error()),
		}
	}

	return &fsDb{
		File: file,
		Path: a,
		Size: stat.Size(),
	}, nil
}

func (fs *fsDb) Write(d *[]uint8) (int64, int64, Error) {
	if fs.File == nil {
		err := fs.WakeUp()

		if err != nil {
			return 0, 0, err
		}
	}

	var err error

	_, err = fs.File.Write(*d)

	if err != nil {
		name := fs.File.Name()

		return 0, 0, &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot write to existing file %s with underlying message: %s", name, err.Error()),
		}
	}

	fs.Size += int64(len(*d))

	return int64(len(*d)), fs.Size, nil
}

func (fs *fsDb) Read(offset int64) (*[]uint8, Error) {
	if fs.File == nil {
		if err := fs.WakeUp(); err != nil {
			return nil, err
		}
	}

	_, e := fs.File.Seek(offset, 0)

	if e != nil {
		return nil, &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Unable to seek index %d with underlying error: %s", offset, e.Error()),
		}
	}

	r := NewLineReader(fs.File)

	_, data, _, err := r.Read()

	if err != nil {
		return nil, err
	}

	return &data.val, nil
}

func (fs *fsDb) Delete(id *[]uint8) Error {
	if fs.File == nil {
		if err := fs.WakeUp(); err != nil {
			return err
		}
	}

	_, e := fs.File.Seek(0, 0)

	if e != nil {
		return &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Unable to delete %s: %s", string(*id), e.Error()),
		}
	}

	or := NewOffsetReader(fs.File)

	found, offset, err := or.GetOffset(string(*id))

	if err != nil {
		return err
	}

	if found {
		_, oe := fs.File.Seek(offset, 0)

		if oe != nil {
			return &dbError{
				Code:    DbErrorCode,
				Message: fmt.Sprintf("Unable to delete %s: %s", string(*id), oe.Error()),
			}
		}

		_, e := fs.File.Write([]uint8(delMark))

		if e != nil {
			return &dbError{
				Code:    DbErrorCode,
				Message: fmt.Sprintf("Unable to delete %s: %s", string(*id), e.Error()),
			}
		}
	}

	return nil
}

func (fs *fsDb) StrategicDelete(id *[]uint8, offset int64) Error {
	if fs.File == nil {
		if err := fs.WakeUp(); err != nil {
			return err
		}
	}

	_, e := fs.File.Seek(offset, 0)

	if e != nil {
		return &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Unable to delete %s: %s", string(*id), e.Error()),
		}
	}

	_, fsErr := fs.File.Write([]uint8(delMark))

	if fsErr != nil {
		return &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Unable to delete %s: %s", string(*id), fsErr.Error()),
		}
	}

	return nil
}


func (fs *fsDb) Sleep() Error {
	if err := fs.SyncAndClose(); err != nil {
		return err
	}

	fs.File = nil

	return nil
}

func (fs *fsDb) WakeUp() Error {
	file, err := createFile(fs.Path, os.O_RDWR)

	if err != nil {
		return err
	}

	fs.File = file

	return nil
}

func (fs *fsDb) SyncAndClose() Error {
	var err error

	name := fs.File.Name()
	err = fs.File.Sync()

	if err != nil {
		return &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Database integrity violation. Database file system problem for file %s with underlying message: %s", name, err.Error()),
		}
	}

	err = fs.File.Close()

	if err != nil {
		return &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot close file %s with underlying message: %s", name, err.Error()),
		}
	}

	return nil
}

