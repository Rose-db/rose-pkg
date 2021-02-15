package rose

import (
	"fmt"
	"os"
	"runtime"
)

func createDbIfNotExists(output bool) (bool, Error) {
	var dir, db, logDir string

	dir = roseDir()
	db = fmt.Sprintf("%s/db", dir)
	logDir = fmt.Sprintf("%s/log", dir)

	dirs := [3]string{dir, db, logDir}
	updated := 0

	if output {
		fmt.Println("\033[32minfo:\033[0m", "Creating the database on the filesystem if not exists...")
	}

	// Create rose directories
	for _, d := range dirs {
		if _, err := os.Stat(d); os.IsNotExist(err) {
			updated++
			fsErr := os.Mkdir(d, os.ModePerm)


			if fsErr != nil {
				return false, newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("      Trying to create directory %s failed with underlying message: %s", d, fsErr.Error()))
			}
		}
	}

	return true, nil
}

func createFile(f string, flag int) (*os.File, Error) {
	file, err := os.OpenFile(f, flag, 0666)

	if err != nil {
		sysErr := newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Error occurred trying to create file %s: %s", f, err.Error()))


		return nil, sysErr
	}

	return file, nil
}

func closeFile(file *os.File) Error {
	fsErr := file.Sync()

	if fsErr != nil {

		return newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Error occurred trying to sync file: %s", fsErr.Error()))

	}

	fsErr = file.Close()

	if fsErr != nil {
		return newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Error occurred trying to close file %s: %s", file.Name(), fsErr.Error()))

	}

	return nil
}

func createIndexLocationIfNotExists() Error {
	idxLoc := roseIndexLocation()

	_, err := os.Stat(idxLoc)

	if os.IsNotExist(err) {
		f, err := createFile(idxLoc, os.O_RDWR|os.O_CREATE)

		if err != nil {
			return newError(masterCode(err.GetMasterCode()), code(err.GetCode()), fmt.Sprintf("A system error occurred and Rose cannot be booted. Cannot create index file. This is probably a permissions error but the error message can give you more details: %s", err.Error()))
		}

		if e := f.Close(); e != nil {
			return newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("A system error occurred and Rose cannot be booted. Unable to close index file: %s", e.Error()))
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

func roseBlockFile(block uint16, dbDir string) string {
	return fmt.Sprintf("%s/block_%d.rose", dbDir, block)
}

func roseIndexLocation() string {
	return fmt.Sprintf("%s/%s", roseDir(), "/indexes.rose")
}