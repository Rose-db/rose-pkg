package rose

import (
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func loadDbInMemory(m *Db, log bool) Error {
	files, fsErr := ioutil.ReadDir(roseDbDir())

	if fsErr != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Could not read %s directory with underlynging message: %s", roseDbDir(), fsErr.Error()),
		}
	}

	errChan := make(chan Error)
	errors := make([]string, 1)

	go func(errChan chan Error) {
		for e := range errChan {
			if e != nil {
				errors = append(errors, e.Error())
			}
		}
	}(errChan)

	limit, err := getOpenFileHandleLimit()

	if err != nil {
		return err
	}

	// Creates as many batches as there are files, 50 files per batch
	batch := createFileInfoBatch(files, limit)


	var bar *pb.ProgressBar
	if log {
		bar = pb.StartNew(len(files))
		bar.SetRefreshRate(time.Millisecond)
	}
	/**
	Every batch has a sender goroutine that sends a single
	file to a receiver goroutine. There can be only 1 sender but
	depending on batch size, there can be {batch_size} receivers.
	*/
	for _, b := range batch {
		dataCh := make(chan os.FileInfo)
		wg := &sync.WaitGroup{}
		lock := &sync.RWMutex{}

		// sender
		go func() {
			for _, f := range b {
				dataCh<- f
			}

			close(dataCh)
		}()

		// receiver
		for i := 0; i < len(b); i++ {
			if log {
				bar.Increment()
			}
			wg.Add(1)
			go loadSingleFile(m, dataCh, wg, errChan, lock)
		}

		wg.Wait()
	}

	close(errChan)

	if len(errors[1:]) > 0 {
		fmt.Printf("Errors occurred while trying to load the database. For brevity, only the first 5 errors are shown here. Go to %s for more information\n", roseLogDir())

		e := errors[1:6]

		for _, err := range e {
			fmt.Println(err)
		}

		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: "Unable to load database into filesystem. Exiting!",
		}
	}

	m.CurrMapIdx = uint16(len(files)) - 1

	if log {
		bar.Finish()
	}

	return nil
}

func loadSingleFile(m *Db, dataCh<- chan os.FileInfo, wg *sync.WaitGroup, errChan chan Error, lock *sync.RWMutex) {
	f := <-dataCh

	db := fmt.Sprintf("%s/%s", roseDbDir(), f.Name())

	file, err := createFile(db, os.O_RDONLY)

	if err != nil {
		fsErr := closeFile(file)

		if fsErr != nil {
			errChan<- fsErr
			wg.Done()
			return
		}

		errChan<- err
		wg.Done()
		return
	}

	reader := NewLineReader(file)

	for {
		offset, val, ok, err := reader.Read()

		if err != nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				errChan<- fsErr
				wg.Done()
				return
			}

			errChan<- &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Database integrity violation. Cannot populate database with message: %s", err.Error()),
			}
			wg.Done()
			return
		}

		if !ok {
			break
		}

		if val == nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				errChan<- fsErr
				wg.Done()
				return
			}

			errChan<- &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: "Database integrity violation. Cannot populate database. Invalid row encountered.",
			}
			wg.Done()
			return
		}

		fileName := f.Name()
		dotSplit := strings.Split(fileName, ".")
		underscoreSplit := strings.Split(dotSplit[0], "_")
		i, _ := strconv.Atoi(underscoreSplit[1])
		mapIdx := uint16(i)

		err = m.writeOnLoad(string(val.id), val.val, mapIdx, lock, offset)

		if err != nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				errChan<- fsErr
				wg.Done()
				return
			}

			errChan<- err
			wg.Done()
			return
		}
	}

	fsErr := closeFile(file)

	if fsErr != nil {
		errChan<- fsErr
		wg.Done()
		return
	}

	errChan<- nil
	wg.Done()
}

func createDbIfNotExists(log bool) (bool, Error) {
	var dir, db, logDir string
	var err Error
	var file *os.File

	dir = roseDir()
	db = fmt.Sprintf("%s/db", dir)
	logDir = fmt.Sprintf("%s/log", dir)

	dirs := [3]string{dir, db, logDir}
	updated := 0

	if log {
		fmt.Println(string("\033[32mINFO:\033[0m"), "Creating the database on the filesystem if not exists...")
	}

	roseDbCreated := false
	// Create rose directories
	for i, d := range dirs {
		if _, err := os.Stat(d); os.IsNotExist(err) {
			if i == 0 {
				roseDbCreated = true
			}

			updated++
			fsErr := os.Mkdir(d, os.ModePerm)
			if fsErr != nil {
				return false, &systemError{
					Code:    SystemErrorCode,
					Message: fmt.Sprintf("Trying to create directory %s failed with underlying message: %s", d, fsErr.Error()),
				}
			}
		}
	}

	if log && updated > 0 && updated != 3 {
		fmt.Println("  Some directories were missing. They have been created again.")
	}

	created := false
	// create first block file
	a := roseBlockFile(0, roseDbDir())
	if _, fsErr := os.Stat(a); os.IsNotExist(fsErr) {
		file, err = createFile(a, os.O_RDWR|os.O_CREATE)

		if err != nil {
			return false, &systemError{
				Code:    SystemErrorCode,
				Message: fmt.Sprintf("Trying to create initial block file failed with underlying message: %s", err.Error()),
			}
		}

		created = true
	}

	if log {
		if created {
			fmt.Printf("\tFilesystem database created for the first time\n\n")

			err = closeFile(file)

			if err != nil {
				return false, &systemError{
					Code:    SystemErrorCode,
					Message: fmt.Sprintf("Trying to close initial block file failed with underlying message: %s", err.Error()),
				}
			}
		} else {
			fmt.Printf("  Filesystem database already exists. Nothing to update\n\n")
		}
	}

	return roseDbCreated, nil
}

func createFile(f string, flag int) (*os.File, Error) {
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

func closeFile(file *os.File) Error {
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

func roseLogDir() string {
	return fmt.Sprintf("%s/.rose_db/log", userHomeDir())
}

func roseBlockFile(block uint16, dbDir string) string {
	return fmt.Sprintf("%s/block_%d.rose", dbDir, block)
}