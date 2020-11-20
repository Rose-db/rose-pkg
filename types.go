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

// 16MB
const maxValSize = 16000000

const delim = "[##]{{}#]"
const delMark = "{[{del}]}"

type ModeType uint8

const InMemoryMode ModeType = 1
const FilesystemMode ModeType = 2
const PartialFilesystemMode ModeType = 3

// default mode must be FilesystemMode mode
type mode struct {
	CurrentMode ModeType
}

func newMode(m ModeType) mode {
	return mode{CurrentMode: m}
}

func (m mode) IsFilesystemMode() bool {
	return m.CurrentMode == FilesystemMode
}

func (m mode) IsInMemoryMode() bool {
	return m.CurrentMode == InMemoryMode
}

func (m mode) IsPartialFilesystemMode() bool {
	return m.CurrentMode == PartialFilesystemMode
}