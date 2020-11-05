package rose

// error types
const systemErrorType = "system_error"
const metadataErrorType = "metadata_error"

// application error codes
const MetadataErrorCode = 1
const SystemErrorCode = 2
const InvalidRequestCode = 3
const DbIntegrityViolationCode = 4

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

// memory db status types
const FreeListQueryStatus = 1
const ExistsStatus = 2
const NewBlockCreatedStatus = 3
const NormalExecutionStatus = 4

// 128B
const maxIdSize = 128
// 16MB
const maxValSize = 16000000

const delim = "[##]{{}#]"