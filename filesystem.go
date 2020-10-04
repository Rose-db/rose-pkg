package rose

import (
	"fmt"
	"os"
	"runtime"
)

func createDbIfNotExists(msgCom chan string, errCom chan RoseError) {
	var roseDb string
	var fsErr RoseError

	roseDb = RoseDir()

	msgCom<- "Creating the database on the filesystem if not exists..."
	if _, err := os.Stat(roseDb); os.IsNotExist(err) {
		msgCom<- "Database not found. Creating it now from scratch..."
		err = os.Mkdir(roseDb, os.ModePerm)

		if err != nil {
			close(msgCom)
			fsErr = &systemError{
				Code:    SystemErrorCode,
				Message: err.Error(),
			}

			errCom<- fsErr

			close(errCom)

			return
		}
	}

	msgCom<- "Filesystem database created successfully"

	close(msgCom)
	close(errCom)
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

func RoseDir() string {
	return fmt.Sprintf("%s/.rose_db", userHomeDir())
}

/**
Creates a file with name {id}.rose and writes the value in it.

This function is designed to be called as a goroutine only.

DO NOT CALL THIS FUNCTION AS ANYTHING ELSE EXCEPT AS A GOROUTINE

This method must write to a file that does not exists. If the file
exists, its content is truncated and replaced with new content. If the file
does not exists, it is created and content is written to it.

This function cannot result in an error and therefor, panics if it encounters
one. If this function panics, its is an indication of an internal bug. Execution
cannot continue if this function fails.
*/
func fsWrite(id uint, d *[]byte) {
	// create file name, for example /path/to/dir/.rose_db/{id}.rose
	f := fmt.Sprintf("%s/%d.rose", RoseDir(), id)

	// if the file exists, work with the existing file and return
	if _, err := os.Stat(f); os.IsExist(err) {
		// if the file exists, empty the file contents
		err = os.Truncate(f, 0)

		if err != nil {
			panic(&dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Database integrity violation. Cannot truncate file %s with underlying message: %s", f, err.Error()),
			})
		}

		// open the file for writing
		file, err := os.Open(f)

		if err != nil {
			panic(&dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Database integrity violation. Cannot open file %s with underlying message: %s", f, err.Error()),
			})
		}

		// write to file
		_, err = file.Write(*d)

		if err != nil {
			panic(&dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Database integrity violation. Cannot write to existing file %s with underlying message: %s", f, err.Error()),
			})
		}

		err = file.Close()

		if err != nil {
			panic(&dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Database integrity violation. Database file system problem for file %s with underlying message: %s", f, err.Error()),
			})
		}

		return
	}

	// if the file doesn't exist, create it
	file, err := os.Create(f)

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot create file %s with underlying message: %s", f, err.Error()),
		})
	}

	_, err = file.Write(*d)

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot write to new file %s with underlying message: %s", f, err.Error()),
		})
	}

	err = file.Close()

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot close file %s with underlying message: %s", f, err.Error()),
		})
	}
}
