package rose

// error types
const systemErrorType = "system_error"
const metadataErrorType = "metadata_error"
const dbErrorType = "db_error"
const dbIntegrityErrorType = "db_integrity_error"

// application error codes
const DataErrorCode = 1
const SystemErrorCode = 2
const DbIntegrityViolationCode = 4
const DbErrorCode = 5

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
const FreeListQueryStatus = 1
const ExistsStatus = 2
const NewBlockCreatedStatus = 3
const NormalExecutionStatus = 4

// 16MB
const maxValSize = 16000000

const delim = "[##]{{}#]"
const delMark = "{[{del}]}"