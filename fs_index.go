package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type indexDataType string

const stringIndexType indexDataType = "string"
const intIndexType indexDataType = "int"
const floatIndexType indexDataType = "float"
const boolIndexType indexDataType = "bool"

type fsIndex struct {
	Name string
	Field string
	DataType indexDataType
	processable boolean
}

func (fsi fsIndex) validate() Error {
	if fsi.Field == "" {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Index field name cannot be an empty string"))
	}

	return nil
}

type indexFsHandler struct {
	file *os.File
	indexes []*fsIndex
}

func newIndexHandler() (*indexFsHandler, Error) {
	f, err := createFile(roseIndexLocation(), os.O_RDWR)

	if err != nil {
		return nil, newError(GenericMasterErrorCode, FilesystemMasterErrorCode, fmt.Sprintf("Cannot open index location. This is an unrecoverable error: %s", err.Error()))
	}

	// it is safe to read all indexes into memory, not expected to be a lot of them (millions)
	b, e := ioutil.ReadAll(f)

	if e != nil {
		return nil, newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Cannot read from index location: %s", e.Error()))
	}

	c := &indexFsHandler{
		file: f,
	}

	if err := c.init(b); err != nil {
		return nil, err
	}

	return c, nil
}

func (ih *indexFsHandler) Add(fsi fsIndex) Error {
	if err := fsi.validate(); err != nil {
		return err
	}

	d := fmt.Sprintf("%s%s%s%s%s\n", fsi.Name, delim, fsi.Field, delim, fsi.DataType)

	if ok := ih.exists(fsi.Name, fsi.Field); !ok {
		if _, err := ih.file.Write([]uint8(d)); err != nil {
			return newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Cannot create write index to filesystem: %s", err.Error()))
		}
	}

	ih.indexes = append(ih.indexes, &fsi)

	return nil
}

// this function is only to be used at boot, it loads all indexes into memory for ease of use, it must not be used
// in other operations
func (ih *indexFsHandler) Find(collName string) ([]*fsIndex, Error) {
	if len(ih.indexes) == 0 {
		return nil, nil
	}

	indexes := make([]*fsIndex, 0)
	for _, a := range ih.indexes {
		if a.Name == collName {
			indexes = append(indexes, a)
		}
	}

	return indexes, nil
}

func (ih *indexFsHandler) Close() Error {
	e := ih.file.Close()

	if e != nil {
		return newError(SystemMasterErrorCode, MalformedIndexCode, fmt.Sprintf("Unable to close index file: %s", e.Error()))
	}

	ih.indexes = nil

	return nil
}

func (ih *indexFsHandler) init(indexes []uint8) Error {
	ih.indexes = make([]*fsIndex, 0)

	if len(indexes) == 0 {
		return nil
	}

	a := string(indexes)

	s := strings.Split(a, "\n")

	for _, a := range s {
		if a != "" {
			t := strings.Split(a, delim)

			if len(t) != 3 {
				return newError(SystemMasterErrorCode, MalformedIndexCode, fmt.Sprintf("Found malformed index value -> %s", a))
			}

			fsi := fsIndex{
				Name:    t[0],
				Field:    t[1],
				DataType: indexDataType(t[2]),
				processable: true,
			}

			ih.indexes = append(ih.indexes, &fsi)
		}
	}

	return nil
}

func (ih *indexFsHandler) exists(collName string, fieldName string) bool {
	for _, idx := range ih.indexes {
		if idx.Name == collName && idx.Field == fieldName {
			return true
		}
	}

	return false
}


