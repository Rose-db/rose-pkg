package rose

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"os"
)

func loadIndexes(dbs map[string]*db, output bool) Error {
	if output {
		fmt.Println("")
		fmt.Println("\033[32mINFO:\033[0m " + "Loading primary index...")
	}

	if err := loadAllIndexes(dbs); err != nil {
		return err
	}

	if output {
		fmt.Println("      Primary index loaded")
		fmt.Println("")
	}

	return nil
}

/**
  1. Iterate over all collections
  2. Create a batch of files for each collection
  3. Create a goroutine for each batch

  - On error, every goroutine working must stop and return the error.
  - on error, every batch and collection iteration must stop and exit with error
*/
func loadAllIndexes(dbs map[string]*db) Error {
	// a filesystem index handler used by this function to get
	// the currently saved indexes
	fsIdx, err := newIndexHandler()

	if err != nil {
		return err
	}

	for collName, db := range dbs {
		files, fsErr := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collName))

		if fsErr != nil {
			return newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Could not read %s directory with underlynging message: %s", roseDbDir(), fsErr.Error()))
		}

		limit, err := getOpenFileHandleLimit()

		if err != nil {
			return err
		}

		// get all indexes from indexes.rose for this collection
		indexes, err := fsIdx.Find(collName)

		// Creates as many batches as there are files, 50 files per batch
		batch := createFileInfoBatch(files, limit)

		/**
		Every batch has a sender goroutine that sends a single
		file to a receiver goroutine. There can be only 1 sender but
		depending on batch size, there can be {batch_size} receivers.
		*/
		for _, b := range batch {
			errs, _ := errgroup.WithContext(context.Background())

			for _, f := range b {
				fileInfo := f
				c := collName
				currentDb := db

				errs.Go(func() error {
					err := loadSingleFile(fileInfo, currentDb, c, indexes)

					if err != nil {
						return err
					}

					return nil
				})
			}

			err := errs.Wait()

			if err != nil {
				return newError(SystemMasterErrorCode, OperatingSystemCode, err.Error())
			}
		}
	}

	if err := fsIdx.Close(); err != nil {
		return err
	}

	return nil
}

func loadSingleFile(f os.FileInfo, m *db, collName string, indexes []*fsIndex) Error {
	db := fmt.Sprintf("%s/%s", fmt.Sprintf("%s/%s", roseDbDir(), collName), f.Name())

	file, err := createFile(db, os.O_RDONLY)

	if err != nil {
		return err
	}

	reader := NewLineReader(file)

	for {
		offset, val, err := reader.Read()

		if err != nil && err.GetCode() == EOFCode {
			break
		}

		if err != nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				return fsErr
			}

			return err
		}

		if val == nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				return fsErr
			}

			return newError(DbIntegrityMasterErrorCode, BlockCorruptedCode, "Database integrity violation. Cannot populate database. Invalid row encountered")
		}

		err = m.writeIndex(val.id, offset)

		// write all indexes into memory in the specified database based on the collection name
		if indexes != nil {
			for _, fsi := range indexes {
				if err := m.writeFieldIndex(fsi.Field, fsi.DataType, offset, val.val); err != nil {
					return err
				}
			}

		}

		if err != nil {
			fsErr := closeFile(file)

			if fsErr != nil {
				return fsErr
			}

			return err
		}
	}

	fsErr := closeFile(file)

	if fsErr != nil {
		return fsErr
	}

	return nil
}