package rose

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

// method types
const InsertMethodType = "insert"
const DeleteMethodType = "delete"
const ReadMethodType = "read"

