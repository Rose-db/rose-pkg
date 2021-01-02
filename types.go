package rose

// error types
const systemErrorType = "system_error"
const metadataErrorType = "metadata_error"
const dbErrorType = "db_error"
const dbIntegrityErrorType = "db_integrity_error"
const validationErrorType = "validation_error"
const timeoutErrorType = "timeout_error"
const endOfFileErrorType = "end_of_file_error"
const queryErrorType = "query_error"

// application error codes
const DataErrorCode = 1
const SystemErrorCode = 2
const DbIntegrityViolationCode = 3
const DbErrorCode = 4
const TooManyOpenFilesCode = 5
const ValidationErrorCode = 6
const TimeoutErrorCode = 7
const EOFErrorCode = 8
const QueryErrorCode = 9

// result status
const OkResultStatus = "ok"
const FoundResultStatus = "found"
const NotFoundResultStatus = "not_found"
const DeletedResultStatus = "deleted"
const ReplacedResultStatus = "replaced"

// method types
const WriteMethodType = "insert"
const DeleteMethodType = "delete"
const ReadMethodType = "read"
const ReplaceMethodType = "replace"

// memory db status types
const NewBlockCreatedStatus = 1
const NormalExecutionStatus = 2

type driverType int

const writeDriver driverType = 1
const updateDriver driverType = 2

// 16MB
const maxValSize = 5000000

const timeoutIteration = 200
const timeoutInterval = 50

const delim = "[##]{{}#]"
const delMark = "{[{del}]}"

const blockMark = 3307
const defragmentMark = 1323

type dataType string

const stringType dataType = "string"
const intType dataType = "int"
const floatType dataType = "float"

func (d dataType) isValid() bool {
	return !(d != stringType && d != intType && d != floatType)
}

type queryType string

const equality queryType = "strictEquality"