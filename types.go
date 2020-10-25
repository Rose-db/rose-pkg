package rose

import (
	"bufio"
	"io"
)

// error types
const systemErrorType = "system_error"
const httpErrorType = "http_error"

// application error codes
const HttpErrorCode = 1
const SystemErrorCode = 2
const InvalidRequestCode = 3
const DbIntegrityViolationCode = 4

// database error codes
const invalidReadErrorCode = 5

// result status
const OkResultStatus = "ok"
const FoundResultStatus = "found"
const NotFoundResultStatus = "not_found"
const DuplicatedIdStatus = "duplicated_id"
const EntryDeletedStatus = "deleted"

// method types
const InsertMethodType = "insert"
const DeleteMethodType = "delete"
const ReadMethodType = "read"

type reader struct {
	internalReader *bufio.Reader
	reader io.Reader
}

type idValue struct {
	id *[]uint8
	val *[]uint8
}

