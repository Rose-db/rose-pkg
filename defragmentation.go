package rose

import (
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"github.com/juju/fslock"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
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

		id, e := strconv.Atoi(string(val.id))

		if e != nil {
			return nil, &systemError{
				Code:    SystemErrorCode,
				Message: fmt.Sprintf("Could not convert string to int during defragmentation with underlying message: %s", e.Error()),
			}
		}

		d := string(prepareData(id, val.val))
		dataToWrite += d
		indexes[id] = index
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

func createBackupDirectory() Error {
	backupDir := fmt.Sprintf("%s/backup", roseDbDir())
	if err := os.Mkdir(backupDir, os.ModePerm); err != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Error trying to create backup directory for defragmentation with undelying message: %s", err.Error()),
		}
	}

	return nil
}

func removeBackupDirectory() Error {
	backupDir := fmt.Sprintf("%s/backup", roseDbDir())

	if err := os.RemoveAll(backupDir); err != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Error trying to remove backup directory for defragmentation with undelying message: %s", err.Error()),
		}
	}

	return nil
}

func removeDbFiles() Error {
	roseDb := roseDbDir()
	files, err := ioutil.ReadDir(roseDb)

	if err != nil {
		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Unable to read file from directory %s for defragmentation with underlying message: %s", roseDb, err.Error()),
		}
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		fileName := fmt.Sprintf("%s/%s", roseDb, f.Name())

		if err := os.Remove(fileName); err != nil {
			return &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Unable to remove file from directory %s for defragmentation with underlying message: %s", roseDb, err.Error()),
			}
		}
	}

	return nil
}

func moveFilesToBackup() Error {
	roseDb := roseDbDir()
	files, err := ioutil.ReadDir(roseDb)

	if err != nil {
		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Unable to read file from directory %s for defragmentation with underlying message: %s", roseDb, err.Error()),
		}
	}

	if len(files) == 0 {
		return nil
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		if err := copyToBackup(f.Name()); err != nil {
			return err
		}
	}

	return nil
}

func copyToBackup(fileName string) Error {
	backupDir := fmt.Sprintf("%s/backup", roseDbDir())
	roseDb := roseDbDir()

	backupFilePath := fmt.Sprintf("%s/%s", backupDir, fileName)
	backupFile, err := createFile(backupFilePath, os.O_RDWR|os.O_CREATE)

	if err != nil {
		return err
	}

	origFilePath := fmt.Sprintf("%s/%s", roseDb, fileName)
	origFile, err := createFile(origFilePath, os.O_RDWR)

	if err != nil {
		return err
	}

	_, fsErr := io.Copy(backupFile, origFile)

	if fsErr != nil {
		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Unable to copy file when defragmenting with underlying message: %s", fsErr.Error()),
		}
	}

	if err := closeFile(backupFile); err != nil {
		return err
	}

	if err := closeFile(origFile); err != nil {
		return err
	}

	return nil
}

func writeBackupToDb(log bool) Error {
	backupDir := fmt.Sprintf("%s/backup", roseDbDir())

	dbDir := roseDbDir()
	m := newDb(newFsDriver(dbDir, writeDriver), newFsDriver(dbDir, updateDriver), newFsDriver(dbDir, updateDriver), "skldfjasčlkdfj")

	files, fsErr := ioutil.ReadDir(backupDir)

	if fsErr != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Could not read %s directory with underlynging message: %s", roseDbDir(), fsErr.Error()),
		}
	}

	var bar *pb.ProgressBar
	if log {
		fmt.Println("")
		bar = pb.Simple.Start(len(files))
		bar.SetRefreshRate(time.Millisecond)
	}

	for _, f := range files {
		if log {
			bar.Increment()
		}

		fileName := fmt.Sprintf("%s/%s", backupDir, f.Name())

		file, err := createFile(fileName, os.O_RDONLY)

		if err != nil {
			return err
		}

		reader := NewLineReader(file)

		for {
			_, val, err := reader.Read()

			if err != nil && err.GetCode() == EOFErrorCode {
				break
			}

			if err != nil {
				fsErr := closeFile(file)

				if fsErr != nil {
					return fsErr
				}

				return &dbIntegrityError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Database integrity violation while defragmenting. Cannot populate database with message: %s", err.Error()),
				}
			}

			if val == nil {
				fsErr := closeFile(file)

				if fsErr != nil {
					return fsErr
				}

				return &dbIntegrityError{
					Code:    DbIntegrityViolationCode,
					Message: "Database integrity violation while defragmenting. Cannot populate database. Invalid row encountered.",
				}
			}

			fileName := f.Name()
			dotSplit := strings.Split(fileName, ".")
			underscoreSplit := strings.Split(dotSplit[0], "_")
			i, _ := strconv.Atoi(underscoreSplit[1])
			mapIdx := uint16(i)

			strId := string(val.id)
			id, atoiErr := strconv.Atoi(strId)

			if atoiErr != nil {
				return &dbIntegrityError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Database integrity violation while defragmenting. Cannot populate database. Encountered an error while converting string to int with underlying message: %s", atoiErr.Error()),
				}
			}

			err = m.writeOnDefragmentation(id, val.val, mapIdx)

			if err != nil {
				fsErr := closeFile(file)

				if fsErr != nil {
					return fsErr
				}

				return err
			}
		}
	}

	if log {
		bar.Finish()
		fmt.Println("")
	}

	return nil
}

func defragment(log bool) Error {
	if log {
		fmt.Printf("  Creating database backup...\n")
	}

	if err := createBackupDirectory(); err != nil {
		return err
	}

	if err := moveFilesToBackup(); err != nil {
		if err := removeBackupDirectory(); err != nil {
			return err
		}

		return err
	}

	if log {
		fmt.Printf("  Backup complete\n")
		fmt.Printf("  Removing database to apply defragmentation...\n")
	}

	if err := removeDbFiles(); err != nil {
		return err
	}

	if log {
		fmt.Printf("  Database removed. Applying defragmentation...\n")
	}

	if err := writeBackupToDb(log); err != nil {
		return err
	}

	if err := removeBackupDirectory(); err != nil {
		return err
	}

	return nil
}