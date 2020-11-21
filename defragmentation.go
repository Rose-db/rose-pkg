package rose

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
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

func moveFilesToBackup() Error {
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

func writeToDb() Error {
	backupDir := fmt.Sprintf("%s/backup", roseDbDir())

	files, fsErr := ioutil.ReadDir(backupDir)

	if fsErr != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Could not read %s directory with underlynging message: %s", roseDbDir(), fsErr.Error()),
		}
	}

	for _, f := range files {
		file, err := createFile(f.Name(), os.O_RDONLY)

		if err != nil {
			return err
		}

		reader := NewLineReader(file)

		for {
			val, ok, err := reader.Read()

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
		}
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
	}

	/*	if err := writeToDb(); err != nil {
			return err
		}*/

	if err := removeBackupDirectory(); err != nil {
		return err
	}

	return nil
}