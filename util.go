package rose

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func prepareData(id int, data interface{}) []uint8 {
	s := []uint8(fmt.Sprintf("%d%s%v%s", id, delim, data, "\n"))

	return s
}

func isJSON(s []uint8) bool {
	var js json.RawMessage
	return json.Unmarshal(s, &js) == nil
}

func validateData(data interface{}) Error {
	l := len(data.(string))
	if l > maxValSize {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Data cannot be larger than %d bytes (16MB), %d bytes given", maxValSize, l))
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

func secureBlockingCreateFile(a string, flag int) (*os.File, Error) {
	it := 0
	var file *os.File
	var err Error

	for {
		file, err = createFile(a, flag)

		if err != nil {
			err = getFsError(err, "create")

			if it == timeoutIteration {
				return nil, err
			}

			if err.GetCode() == TooManyFilesOpenCode {
				continue
			}

			time.Sleep(timeoutInterval * time.Millisecond)
			it++

			continue
		}

		return file, nil
	}
}

func secureBlockingWriteFile(f *os.File, d []uint8) Error {
	it := 0
	var err error

	for {
		_, err = f.Write(d)

		if err != nil {
			e := getFsError(err, "write")

			if it == timeoutIteration {
				return getFsError(err, "write")
			}

			if e.GetCode() == TooManyFilesOpenCode {
				continue
			}

			time.Sleep(timeoutInterval * time.Millisecond)
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

			if it == timeoutIteration {
				return getFsError(err, "write")
			}

			if e.GetCode() == TooManyFilesOpenCode {
				continue
			}

			time.Sleep(timeoutInterval * time.Millisecond)
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

			if it == timeoutIteration {
				return getFsError(err, "writeAt")
			}

			if e.GetCode() == TooManyFilesOpenCode {
				continue
			}

			time.Sleep(timeoutInterval * time.Millisecond)
			it++

			continue
		}

		return nil
	}
}

func getFsError(err error, op string) Error {
	msg := err.Error()

	if strings.Contains(msg, "too many open files") {
		return newError(FilesystemMasterErrorCode, TooManyFilesOpenCode, fmt.Sprintf("Filesystem timeout error. Cannot do %s file operation with underlying message: %s", op, msg))

	}

	return newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Database integrity violation. Cannot do %s file operation with underlying message: %s", op, msg))
}

func createDateFromString(parts []string) time.Time {
	if len(parts) == 3 {
		year, _ := strconv.Atoi(parts[0])
		month, _ := strconv.Atoi(parts[1])
		day, _ := strconv.Atoi(parts[2])

		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	}

	year, _ := strconv.Atoi(parts[0])
	month, _ := strconv.Atoi(parts[1])
	day, _ := strconv.Atoi(parts[2])
	hour, _ := strconv.Atoi(parts[3])
	min, _ := strconv.Atoi(parts[4])
	sec, _ := strconv.Atoi(parts[5])

	return time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC)
}

func getDateFromString(s string) time.Time {
	sp := strings.Split(s, " ")

	if len(sp) == 2 {
		t := strings.Split(sp[0], "-")
		p := strings.Split(sp[1], ":")

		t = append(t, p...)

		return createDateFromString(t)
	}

	sp = strings.Split(s,"-")

	return createDateFromString(sp)
}



