package rose

// master codes
const FilesystemMasterErrorCode = 1
const ValidationMasterErrorCode = 2
const DbIntegrityMasterErrorCode = 3
const SystemMasterErrorCode = 4
const GenericMasterErrorCode = 5

// application error codes
const IndexNotExistsCode = 1
const FsPermissionsCode = 2
const TooManyFilesOpenCode = 3
const InvalidUserSuppliedDataCode = 4
const EOFCode = 5
const DataConversionCode = 6
const DocumentNotFoundCode = 7
const UnmarshalFailCode = 8
const AppInvalidUsageCode = 9
const ShutdownFailureCode = 10
const BlockCorruptedCode = 11
const OperatingSystemCode = 12

// result status
const OkResultStatus = "ok"
const FoundResultStatus = "found"
const NotFoundResultStatus = "not_found"
const DeletedResultStatus = "deleted"
const ReplacedResultStatus = "replaced"

// method types
const WriteMethodType = "insert"
const BulkWriteMethodType = "bulkWrite"
const DeleteMethodType = "delete"
const ReadMethodType = "read"
const ReplaceMethodType = "replace"

// memory db status types
const NormalExecutionStatus = 1

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
const boolType dataType = "bool"
const dateType dataType = "date"
const dateTimeType dataType = "date_time"

func (d dataType) isValid() bool {
	return !(d != stringType && d != intType && d != floatType && d != boolType)
}

func (d dataType) isType(types ...dataType) bool {
	for _, t := range types {
		if t == d {
			return true
		}
	}

	return false
}

type comparisonType string

const equality comparisonType = "eq"
const inequality comparisonType = "neq"
const less comparisonType = "less"
const more comparisonType = "more"
const lessEqual comparisonType = "lessEqual"
const moreEqual comparisonType = "moreEqual"
