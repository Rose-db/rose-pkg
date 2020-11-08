package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

func loadDbInMemory(m *Db, log bool) Error {
	files, fsErr := ioutil.ReadDir(roseDbDir())

	if fsErr != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fsErr.Error(),
		}
	}

	// Creates as many batches as there are files, 50 files per batch
	batch := createFileInfoBatch(files, 10)

	/**
		Every batch has a sender goroutine that sends a single
		file to a receiver goroutine. There can be only 1 sender but
		depending on batch size, there can be {batch_size} receivers.
	 */
	for _, b := range batch {
		dataCh := make(chan os.FileInfo)
		wg := &sync.WaitGroup{}

		// sender
		go func() {
			for _, f := range b {
				dataCh<- f
			}

			close(dataCh)
		}()

		// receiver
		for i := 0; i < len(b); i++ {
			wg.Add(1)
			go loadSingleFile(m, dataCh, wg)
		}

		wg.Wait()
	}

	m.CurrMapIdx = uint16(len(files)) - 1

	return nil
}

func loadSingleFile(m *Db, dataCh<- chan os.FileInfo, wg *sync.WaitGroup) {
	f := <-dataCh
	db := fmt.Sprintf("%s/%s", roseDbDir(), f.Name())

	file, err := createFile(db, os.O_RDONLY)

	if err != nil {
		fsErr := closeFile(file)

		if fsErr != nil {
			wg.Done()

			panic(fsErr)
		}

		wg.Done()
		panic(err)
	}

	reader := NewLineReader(file)

	for {
		val, ok, err := reader.Read()

		if err != nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				wg.Done()

				panic(fsErr)
			}

			wg.Done()

			panic(&dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Database integrity violation. Cannot populate database with message: %s", err.Error()),
			})
		}

		if !ok {
			break
		}

		if val == nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				wg.Done()

				panic(fsErr)
			}

			wg.Done()

			panic(&dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: "Database integrity violation. Cannot populate database. Invalid row encountered.",
			})
		}

		fileName := f.Name()
		dotSplit := strings.Split(fileName, ".")
		underscoreSplit := strings.Split(dotSplit[0], "_")
		i, _ := strconv.Atoi(underscoreSplit[1])
		mapIdx := uint16(i)

		err = m.writeOnLoad(string(val.id), val.val, mapIdx)

		if err != nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				wg.Done()

				panic(fsErr)
			}

			wg.Done()

			panic(err)
		}
	}

	fsErr := closeFile(file)

	if fsErr != nil {
		wg.Done()

		panic(fsErr)
	}

	wg.Done()
}

func createDbIfNotExists(logging bool, comm chan string, errChan chan Error) {
	var dir, db, log string
	var err Error
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

func roseBlockFile(block uint16) string {
	return fmt.Sprintf("%s/block_%d.rose", roseDbDir(), block)
}