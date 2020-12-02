package rose

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

func prepareData(id int, data []uint8) *[]uint8 {
	s := []uint8(fmt.Sprintf("%d%s%s%s", id, delim, string(data), "\n"))

	return &s
}

func isJSON(s []uint8) bool {
	var js json.RawMessage
	return json.Unmarshal(s, &js) == nil
}

func appendByte(slice []uint8, data ...uint8) []uint8 {
	m := len(slice)
	n := m + len(data)
	if n > cap(slice) { // if necessary, reallocate
		// allocate double what's needed, for future growth.
		newSlice := make([]uint8, (n+1)*2)
		copy(newSlice, slice)
		slice = newSlice
	}

	slice = slice[0:n]
	copy(slice[m:n], data)
	return slice
}

func validateData(data []uint8) Error {
	if !isJSON(data) {
		return &dataError{
			Code:    DataErrorCode,
			Message: "Data must be a JSON byte array",
		}
	}

	l := len(data)
	if l > maxValSize {
		return &dataError{
			Code:    DataErrorCode,
			Message: fmt.Sprintf("Data cannot be larger than 16000000 bytes (16MB), %d bytes given", l),
		}
	}

	return nil
}


/**
	Creates batches based on {size}. If len(files) < size,
	it creates a single batch with len(files) in that single batch
 */
func createFileInfoBatch(files []os.FileInfo, size int) map[int][]os.FileInfo {
	m := make(map[int][]os.FileInfo)

	currBatch := 0
	idx := 0
	batch := []os.FileInfo{}
	for i, f := range files {
		if i != 0 && i % size == 0 {
			idx = 0
			m[currBatch] = batch
			currBatch++
			batch = []os.FileInfo{}

			batch = append(batch, f)
		} else {
			batch = append(batch, f)
			idx++
		}
	}

	if len(batch) > 0 {
		m[currBatch] = batch
	}

	return m
}

func secureBlockingCreateFile(a string) (*os.File, Error) {
	it := 0
	var file *os.File
	var err Error

	for {
		file, err = createFile(a, os.O_RDWR|os.O_CREATE)

		if err != nil {
			err = getFsError(err, "create")

			if it == 200 {
				return nil, err
			}

			if err.GetCode() == TooManyOpenFiles {
				continue
			}

			time.Sleep(50 * time.Millisecond)
			it++

			continue
		}

		return file, nil
	}
}

func secureBlockingWriteFile(f *os.File, d *[]uint8) Error {
	it := 0
	var err error

	for {
		_, err = f.Write(*d)

		if err != nil {
			e := getFsError(err, "write")

			if it == 200 {
				return getFsError(err, "write")
			}

			if e.GetCode() == TooManyOpenFiles {
				continue
			}

			time.Sleep(50 * time.Millisecond)
			it++

			continue
		}

		return nil
	}
}

func secureBlockingSeekFile(f *os.File, offset int64) Error {
	it := 0
	var err error

	for {
		_, err = f.Seek(offset, 0)

		if err != nil {
			e := getFsError(err, "write")

			if it == 200 {
				return getFsError(err, "write")
			}

			if e.GetCode() == TooManyOpenFiles {
				continue
			}

			time.Sleep(50 * time.Millisecond)
			it++

			continue
		}

		return nil
	}
}

func secureBlockingWriteAtFile(f *os.File, d []uint8, offset int64) Error {
	it := 0
	var err error

	for {
		_, err = f.WriteAt(d, offset)

		if err != nil {
			e := getFsError(err, "writeAt")

			if it == 200 {
				return getFsError(err, "writeAt")
			}

			if e.GetCode() == TooManyOpenFiles {
				continue
			}

			time.Sleep(50 * time.Millisecond)
			it++

			continue
		}

		return nil
	}
}

func getFsError(err error, op string) Error {
	msg := err.Error()

	if strings.Contains(msg, "too many open files") {
		return &systemError{
			Code:    TooManyOpenFiles,
			Message: fmt.Sprintf("Operating system error. Cannot do %s file operation with underlying message: %s", op, msg),
		}
	}

	return &dbError{
		Code:    DbIntegrityViolationCode,
		Message: fmt.Sprintf("Database integrity violation. Cannot do %s file operation with underlying message: %s", op, msg),
	}
}