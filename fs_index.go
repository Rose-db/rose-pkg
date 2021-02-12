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
}

func (fsi fsIndex) validate() Error {
	if fsi.Field == "" {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Index field name cannot be an empty string"))
	}

	return nil
}

type indexFsHandler struct {
	file *os.File
}

func (ih *indexFsHandler) Add(fsi fsIndex) Error {
	if err := fsi.validate(); err != nil {
		return err
	}

	d := fmt.Sprintf("%s%s%s%s%s\n", fsi.Name, delim, fsi.Field, delim, fsi.DataType)

	if _, err := ih.file.Write([]uint8(d)); err != nil {
		return newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Cannot create write index to filesystem: %s", err.Error()))
	}

	return nil
}

// this function is only to be used at boot, it loads all indexes into memory for ease of use, it must not be used
// in other operations
func (ih *indexFsHandler) Find(collName string) (*fsIndex, Error) {
	// it is safe to read all indexes into memory, not expected to be a lot of them (millions)
	b, err := ioutil.ReadAll(ih.file)

	if err != nil {
		return nil, newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Cannot read from index location: %s", err.Error()))
	}

	s := strings.Split(string(b), "\n")

	for _, a := range s {
		t := strings.Split(a, delim)

		if len(t) != 3 {
			return nil, newError(SystemMasterErrorCode, MalformedIndexCode, fmt.Sprintf("Found malformed index value -> %s", a))
		}

		if t[0] == collName {
			return &fsIndex{
				Name:    t[0],
				Field:    t[1],
				DataType: indexDataType(t[2]),
			}, nil
		}
	}

	return nil, nil
}

func newIndexHandler() (*indexFsHandler, Error) {
	f, err := createFile(roseIndexLocation(), os.O_RDWR)

	if err != nil {
		return nil, newError(GenericMasterErrorCode, FilesystemMasterErrorCode, fmt.Sprintf("Cannot open index location. This is an unrecoverable error: %s", err.Error()))
	}

	return &indexFsHandler{
		file: f,
	}, nil
}


