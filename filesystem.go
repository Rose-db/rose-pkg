package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
)

func populateDb(m *memDb) RoseError {
	files, fsErr := ioutil.ReadDir(roseDbDir())

	if fsErr != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fsErr.Error(),
		}
	}

	for _, f := range files {
		db := fmt.Sprintf("%s/%s", roseDbDir(), f.Name())

		file, err := createFile(db, os.O_RDONLY)

		if err != nil {
			return err
		}

		reader := NewReader(file)

		for {
			val, ok, err := reader.Read()

			if err != nil {
				return &dbIntegrityError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Database integrity violation. Cannot populate database with message: %s", err.Error()),
				}
			}

			if !ok {
				break
			}

			m.Write(string(val.id), val.val)
		}

		fsErr := closeFile(file)

		if fsErr != nil {
			return fsErr
		}
	}

	return nil
}

func createDbIfNotExists(logging bool, comm chan string, errChan chan RoseError) {
	var dir, db, log string
	var err RoseError
	var file *os.File

	dir = roseDir()
	db = fmt.Sprintf("%s/db", dir)
	log = fmt.Sprintf("%s/log", dir)

	dirs := [3]string{dir, db, log}
	updated := 0

	if logging {
		comm<- "Creating the database on the filesystem if not exists..."
	}

	// Create rose directories
	for _, d := range dirs {
		if _, err := os.Stat(d); os.IsNotExist(err) {
			updated++
			fsErr := os.Mkdir(d, os.ModePerm)
			if fsErr != nil {
				close(comm)

				errChan<- &systemError{
					Code:    SystemErrorCode,
					Message: fsErr.Error(),
				}

				close(errChan)

				return
			}
		}
	}

	if logging && updated > 0 && updated != 3 {
		comm<- "Some directories were missing. They have been created again."
	}

	created := false
	// create first block file
	a := roseBlockFile(0)
	if _, fsErr := os.Stat(a); os.IsNotExist(fsErr) {
		file, err = createFile(a, os.O_RDWR|os.O_CREATE)

		if err != nil {
			close(comm)

			errChan<- err

			close(errChan)

			return
		}

		created = true
	}

	if logging {
		if created {
			comm<- "Filesystem database created for the first time"

			err = closeFile(file)

			if err != nil {
				close(comm)

				errChan<- err

				close(errChan)

				return
			}
		} else {
			comm<- "Filesystem database already exists. Nothing to update"
		}
	}

	close(comm)
	close(errChan)
}

func createFile(f string, flag int) (*os.File, RoseError) {
	file, err := os.OpenFile(f, flag, 0666)

	if err != nil {
		sysErr := &systemError{
			Code:    SystemErrorCode,
			Message: err.Error(),
		}

		return nil, sysErr
	}

	return file, nil
}

func closeFile(file *os.File) RoseError {
	fsErr := file.Sync()

	if fsErr != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fsErr.Error(),
		}
	}

	fsErr = file.Close()

	if fsErr != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fsErr.Error(),
		}
	}

	return nil
}

// Returns the directory name of the user home directory.
// Directory returned does not have a leading slash, e.i /path/to/dir
func userHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}

		return home
	} else if runtime.GOOS == "linux" {
		home := os.Getenv("XDG_CONFIG_HOME")
		if home != "" {
			return home
		}
	}

	return os.Getenv("HOME")
}

func roseDir() string {
	return fmt.Sprintf("%s/.rose_db", userHomeDir())
}

func roseDbDir() string {
	return fmt.Sprintf("%s/.rose_db/db", userHomeDir())
}

func roseBlockFile(block uint16) string {
	return fmt.Sprintf("%s/block_%d.rose", roseDbDir(), block)
}