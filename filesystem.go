package rose

import (
	"fmt"
	"os"
	"runtime"
)

func populateDb(m *memDb, file *os.File) {
	reader := NewReader(file)

	for {
		val, ok, err := reader.Read()

		if err != nil {
			panic(err)
		}

		if !ok {
			return
		}

		m.Write(string(*val.id), val.val)
	}
}

func createDbIfNotExists(logging bool) *os.File {
	var dir, db, log string
	var fsErr RoseError
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
			err = os.Mkdir(d, os.ModePerm)
			if err != nil {
				fsErr = &systemError{
					Code:    SystemErrorCode,
					Message: err.Error(),
				}

				panic(fsErr)
			}
		}
	}

	a := fmt.Sprintf("%s/db/rose.rose", dir)
	if _, err := os.Stat(a); os.IsNotExist(err) {
		file = createFile(a, os.O_RDWR|os.O_CREATE)
	} else {
		file = createFile(a, os.O_RDWR)
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

	return file
}

func createFile(f string, flag int) *os.File {
	file, err := os.OpenFile(f, flag, 0666)

	if err != nil {
		sysErr := &systemError{
			Code:    SystemErrorCode,
			Message: err.Error(),
		}

		panic(sysErr)
	}

	return file
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