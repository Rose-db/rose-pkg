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

func newFsDb(b uint16, dbDir string, perms int) (*fsDb, Error) {
	a := roseBlockFile(b, dbDir)
	var file *os.File
	var err Error

	file, err = createFile(a, perms)

	if err != nil && strings.Contains(err.Error(), "too many open") {
		file, err = secureBlockingCreateFile(a)

		if err != nil {
			return nil, err
		}
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

func (fs *fsDb) Write(d []uint8) (int64, int64, Error) {
	_, err := fs.File.Write(d)

	if err != nil {
		e := secureBlockingWriteFile(fs.File, d)

		if e != nil {
			return 0, 0, e
		}
	}

	fs.Size += int64(len(d))

	return int64(len(d)), fs.Size, nil
}

func (fs *fsDb) Read(offset int64) (*[]uint8, Error) {
	_, err := fs.File.Seek(offset, 0)

	if err != nil {
		e := secureBlockingSeekFile(fs.File, offset)

		if e != nil {
			return nil, e
		}
	}

	r := NewLineReader(fs.File)

	_, data, _, e := r.Read()

	if e != nil {
		return nil, e
	}

	if data == nil {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: "Document not found",
		}
	}

	return &data.val, nil
}

func (fs *fsDb) StrategicDelete(id []uint8, del []uint8, offset int64) Error {
	_, err := fs.File.WriteAt(del, offset)

	if err != nil {
		e := secureBlockingWriteAtFile(fs.File, []uint8(delMark), offset)

		if e != nil {
			return e
		}
	}

	if err != nil {
		return &dbError{
			Code:    DbErrorCode,
			Message: fmt.Sprintf("Unable to delete %s with underlying message: %s", string(id), err.Error()),
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

