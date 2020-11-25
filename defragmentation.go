package rose

import (
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

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

	m := newMemoryDb(newFsDriver(roseDbDir()))

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
			_, val, ok, err := reader.Read()

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

			if !ok {
				break
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