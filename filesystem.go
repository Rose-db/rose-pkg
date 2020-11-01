package rose

import (
	"fmt"
	"os"
	"runtime"
)

func populateDb(m *memDb, file *os.File) RoseError {
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
			return nil
		}

		m.Write(string(*val.id), val.val)
	}
}

func createDbIfNotExists(logging bool) (*os.File, RoseError) {
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

	for _, d := range dirs {
		if _, err := os.Stat(d); os.IsNotExist(err) {
			updated++
			fsErr := os.Mkdir(d, os.ModePerm)
			if fsErr != nil {
				return nil, &systemError{
					Code:    SystemErrorCode,
					Message: fsErr.Error(),
				}
			}
		}
	}

	a := roseBlockFile(1)
	if _, fsErr := os.Stat(a); os.IsNotExist(fsErr) {
		file, err = createFile(a, os.O_RDWR|os.O_CREATE)

		if err != nil {
			return nil, err
		}
	} else {
		file, err = createFile(a, os.O_RDWR)

		if err != nil {
			return nil, err
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

	return file, nil
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