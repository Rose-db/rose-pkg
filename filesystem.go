package rose

import (
	"fmt"
	"os"
	"runtime"
)

func populateDb(m *memDb) RoseError {
	a := roseBlockFile(1)

	file, err := createFile(a, os.O_RDWR)

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

		m.Write(string(*val.id), val.val)
	}

	fsErr := closeFile(file)

	if fsErr != nil {
		return fsErr
	}

	return nil
}

func createDbIfNotExists(logging bool) RoseError {
	var dir, db, log string
	var err RoseError
	var file *os.File

	dir = roseDir()
	db = fmt.Sprintf("%s/db", dir)
	log = fmt.Sprintf("%s/log", dir)

	dirs := [3]string{dir, db, log}
	updated := 0

	if logging {
		fmt.Println("Creating the database on the filesystem if not exists...")
	}

	// Create rose directories
	for _, d := range dirs {
		if _, err := os.Stat(d); os.IsNotExist(err) {
			updated++
			fsErr := os.Mkdir(d, os.ModePerm)
			if fsErr != nil {
				return &systemError{
					Code:    SystemErrorCode,
					Message: fsErr.Error(),
				}
			}
		}
	}

	// create first block file
	a := roseBlockFile(1)
	if _, fsErr := os.Stat(a); os.IsNotExist(fsErr) {
		file, err = createFile(a, os.O_RDWR|os.O_CREATE)

		if err != nil {
			return err
		}
	}

	if logging {
		if updated == 3 {
			fmt.Println("Filesystem database created successfully")
		} else if updated == 0 {
			fmt.Println("Filesystem database already exists. Nothing to update")
		} else {
			fmt.Println("Some directories for the filesystem database were missing but were successfully updated")
		}
	}

	err = closeFile(file)

	if err != nil {
		return err
	}

	return nil
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

func roseBlockFile(block int) string {
	return fmt.Sprintf("%s/block_%d.rose", roseDbDir(), block)
}