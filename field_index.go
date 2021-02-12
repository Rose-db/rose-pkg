package rose

type specificIndex struct {
	Pos int64
	Value interface{}
}

type fieldIndex struct {
	DataType indexDataType
	Index map[string][]*specificIndex
}

func newFieldIndex(dataType indexDataType) *fieldIndex {
	return &fieldIndex{DataType: dataType}
}
