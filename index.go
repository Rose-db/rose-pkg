package rose

import (
	"fmt"
	"os"
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

func newIndexHandler() (*indexFsHandler, Error) {
	f, err := createFile(roseIndexLocation(), os.O_RDWR)

	if err != nil {
		return nil, newError(GenericMasterErrorCode, FilesystemMasterErrorCode, fmt.Sprintf("Cannot open index location. This is an unrecoverable error: %s", err.Error()))
	}

	return &indexFsHandler{
		file: f,
	}, nil
}


