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

type fsDbHandler struct {
	File *os.File
	Block uint
}

func newFsDbHandler() *fsDbHandler {
	return &fsDbHandler{}
}

func (fs *fsDbHandler) AcquireBlock(b uint) {
	if fs.File == nil {
		fs.File = fs.createFile(b)
	}

	if b != fs.Block {
		fs.syncAndClose()
		fs.File = fs.createFile(b)
		fs.Block = b
	}
}

func (fs *fsDbHandler) Write(id uint, d *[]byte) {
	var err error
	var name string

	name = fs.File.Name()
	*d = append(*d, byte(10))

	_, err = fs.File.Write(*d)

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot write to existing file %s with underlying message: %s", name, err.Error()),
		})
	}
}

func (fs *fsDbHandler) createFile(b uint) *os.File {
	var f string
	var file *os.File

	f = fmt.Sprintf("%s/%d.rose", RoseDir(), b)

	file, err := os.Create(f)

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot create file %s with underlying message: %s", f, err.Error()),
		})
	}

	return file
}

func (fs *fsDbHandler) syncAndClose() {
	var err error
	var name string

	name = fs.File.Name()
	err = fs.File.Sync()

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Database file system problem for file %s with underlying message: %s", name, err.Error()),
		})
	}

	err = fs.File.Close()

	if err != nil {
		panic(&dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot close file %s with underlying message: %s", name, err.Error()),
		})
	}
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