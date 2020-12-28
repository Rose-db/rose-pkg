package rose

import (
	"fmt"
	"github.com/juju/fslock"
	"io/ioutil"
	"os"
)

func defragmentBlock(blockId uint16, collName string) (map[int]int64, Error) {
	collDir := fmt.Sprintf("%s/%s", roseDbDir(), collName)
	origFileName := roseBlockFile(blockId, collDir)

	l := fslock.New(origFileName)

	e := l.TryLock()
	if e != nil {
		return nil, &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Unable to lock file %s with underlying message: %s", origFileName, e.Error()),
		}
	}

	origFile, err := createFile(origFileName, os.O_RDWR)

	if err != nil {
		return nil, err
	}

	reader := NewLineReader(origFile)

	dataToWrite := ""
	indexes := make(map[int]int64)
	var index int64 = 0
	for {
		_, val, err := reader.Read()

		if err != nil && err.GetCode() == EOFErrorCode {
			break
		}

		if err != nil {
			e = l.Unlock()
			if e != nil {
				return nil, &systemError{
					Code:    SystemErrorCode,
					Message: fmt.Sprintf("Unable to lock file %s with underlying message: %s", origFileName, e.Error()),
				}
			}

			fsErr := closeFile(origFile)

			if fsErr != nil {
				return nil, fsErr
			}

			return nil, &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Database integrity violation while defragmenting with underlying message: %s", err.Error()),
			}
		}

		if val == nil {
			e = l.Unlock()
			if e != nil {
				return nil, &systemError{
					Code:    SystemErrorCode,
					Message: fmt.Sprintf("Unable to lock file %s with underlying message: %s", origFileName, e.Error()),
				}
			}

			fsErr := closeFile(origFile)

			if fsErr != nil {
				return nil, fsErr
			}

			return nil, &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: "Database integrity violation while defragmenting. Invalid row encountered.",
			}
		}

		d := string(prepareData(val.id, val.val))
		dataToWrite += d
		indexes[val.id] = index
		index += int64(len(d))
	}

	e = ioutil.WriteFile(origFileName, []uint8(dataToWrite), 0666)

	if e != nil {
		e = l.Unlock()
		if e != nil {
			return nil, &systemError{
				Code:    SystemErrorCode,
				Message: fmt.Sprintf("Unable to lock file %s with underlying message: %s", origFileName, e.Error()),
			}
		}

		return nil, &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Could not convert string to int during defragmentation with underlying message: %s", e.Error()),
		}
	}

	if err := closeFile(origFile); err != nil {
		e = l.Unlock()
		if e != nil {
			return nil, &systemError{
				Code:    SystemErrorCode,
				Message: fmt.Sprintf("Unable to lock file %s with underlying message: %s", origFileName, e.Error()),
			}
		}

		return nil, err
	}

	e = l.Unlock()
	if e != nil {
		return nil, &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Unable to lock file %s with underlying message: %s", origFileName, e.Error()),
		}
	}

	return indexes, nil
}