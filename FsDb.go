package rose

import (
	"fmt"
	"os"
	"strings"
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
	_, err := fs.File.Write(*d)

	if err != nil {
		name := fs.File.Name()
		msg := err.Error()

		if strings.Contains(msg, "too many open files") {
			return 0, 0, &systemError{
				Code:    TooManyOpenFiles,
				Message: fmt.Sprintf("Operating system error. Cannot write to existing file %s with underlying message: %s", name, msg),
			}
		}

		return 0, 0, &dbError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot write to existing file %s with underlying message: %s", name, msg),
		}
	}

	fs.Size += int64(len(*d))

	return int64(len(*d)), fs.Size, nil
}

func (fs *fsDb) Read(offset int64) (*[]uint8, Error) {
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

func (fs *fsDb) StrategicDelete(id *[]uint8, offset int64) Error {
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

func (fs *fsDb) SyncAndClose() Error {
	var err error

	name := fs.File.Name()

	err = fs.File.Close()

	if err != nil {
		return &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot close file %s with underlying message: %s", name, err.Error()),
		}
	}

	return nil
}

