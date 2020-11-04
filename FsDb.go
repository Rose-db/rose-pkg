package rose

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type fsDb struct {
	Path string
	File *os.File
}

func newFsDb(b uint16) (*fsDb, RoseError) {
	a := roseBlockFile(b)

	file, err := createFile(a, os.O_RDWR|os.O_CREATE)

	if err != nil {
		return nil, err
	}

	return &fsDb{
		File: file,
		Path: a,
	}, nil
}

func (fs *fsDb) Write(d *[]uint8) RoseError {
	if fs.File == nil {
		err := fs.WakeUp()

		if err != nil {
			return err
		}
	}

	var err error

	_, err = fs.File.Write(*d)

	if err != nil {
		name := fs.File.Name()

		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot write to existing file %s with underlying message: %s", name, err.Error()),
		}
	}

	return nil
}

func (fs *fsDb) Sleep() RoseError {
	if err := fs.SyncAndClose(); err != nil {
		return err
	}

	fs.File = nil

	return nil
}

func (fs *fsDb) WakeUp() RoseError {
	file, err := createFile(fs.Path, os.O_RDWR)

	if err != nil {
		return err
	}

	fs.File = file

	return nil
}

func (fs *fsDb) Delete(id *[]uint8) RoseError {
	if fs.File == nil {
		if err := fs.WakeUp(); err != nil {
			return err
		}
	}

	return nil
}

func (fs *fsDb) SyncAndClose() RoseError {
	var err error
	var name string

	name = fs.File.Name()
	err = fs.File.Sync()

	if err != nil {
		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Database file system problem for file %s with underlying message: %s", name, err.Error()),
		}
	}

	err = fs.File.Close()

	if err != nil {
		return &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Database integrity violation. Cannot close file %s with underlying message: %s", name, err.Error()),
		}
	}

	return nil
}

func (fs *fsDb) getOffset(givenId string, reader bufio.Reader) (uint64, RoseError) {
	id := make([]uint8, 1)

	var offset uint64 = 0
	extracting := false
	idFound := false
	for {
		b, err := reader.ReadByte()

		if err == io.EOF {
			return 0, nil
		}

		if err != nil {
			return 0, &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Rose encountered an error tring to delete an entry with message: %s", err.Error()),
			}
		}

		if b == 10 && !idFound {
			offset++

			continue
		}

		if idFound {
			offset++

			continue
		}

		if !extracting {
			if _, err := reader.ReadByte(); err != nil {
				return 0, &dbIntegrityError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Rose encountered an error tring to delete an entry with message: %s", err.Error()),
				}
			}

			if _, err := reader.ReadByte(); err != nil {
				return 0, &dbIntegrityError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Rose encountered an error tring to delete an entry with message: %s", err.Error()),
				}
			}

			extracting = true

			continue
		}

		if extracting && b == 93 {
			z, err := reader.ReadByte()
			if err != nil {
				return 0, &dbIntegrityError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Rose encountered an error tring to delete an entry with message: %s", err.Error()),
				}
			}

			if z != 35 {
				id = append(id, b)
				id = append(id, z)

				continue
			}

			t, err := reader.ReadByte()
			if err != nil {
				return 0, &dbIntegrityError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Rose encountered an error tring to delete an entry with message: %s", err.Error()),
				}
			}

			if t != 93 {
				id = append(id, b)
				id = append(id, z)
				id = append(id, t)

				continue
			}

			extracting = false

			if givenId == string(id) {
				idFound = true

				continue
			}

			id = make([]uint8, 1)
		}

		if extracting && !idFound {
			id = append(id, b)
		}
	}

	return offset, nil
}

