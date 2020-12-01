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

	file, err := secureBlockingCreateFile(a)

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
	err := secureBlockingWriteFile(fs.File, d)

	if err != nil {
		return 0, 0, err
	}

	fs.Size += int64(len(*d))

	return int64(len(*d)), fs.Size, nil
}

func (fs *fsDb) Read(offset int64) (*[]uint8, Error) {
	e := secureBlockingSeekFile(fs.File, offset)

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
	err := secureBlockingWriteAtFile(fs.File, []uint8(delMark), offset)

	if err != nil {
		return &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Unable to delete %s with underlying message: %s", string(*id), err.Error()),
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

