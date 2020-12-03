package rose

// error types
const systemErrorType = "system_error"
const metadataErrorType = "metadata_error"
const dbErrorType = "db_error"
const dbIntegrityErrorType = "db_integrity_error"
const validationErrorType = "validation_error"

// application error codes
const DataErrorCode = 1
const SystemErrorCode = 2
const DbIntegrityViolationCode = 3
const DbErrorCode = 4
const TooManyOpenFilesCode = 5
const ValidationErrorCode = 6

// result status
const OkResultStatus = "ok"
const FoundResultStatus = "found"
const NotFoundResultStatus = "not_found"
const DeletedResultStatus = "deleted"

// method types
const WriteMethodType = "insert"
const DeleteMethodType = "delete"
const ReadMethodType = "read"

// memory db status types
const NewBlockCreatedStatus = 1
const NormalExecutionStatus = 2

// 16MB
const maxValSize = 16000000

const delim = "[##]{{}#]"
const delMark = "{[{del}]}"

const blockMark = 2999