package rose

type specificIndex struct {
	Pos int64
	Value interface{}
	BlockId uint16
}

type fieldIndex struct {
	DataType indexDataType
	Index[]specificIndex
}

func newFieldIndex(dataType indexDataType) *fieldIndex {
	return &fieldIndex{
		DataType: dataType,
		Index: make([]specificIndex, 0),
	}
}

func (fi *fieldIndex) Add(pos int64, value interface{}, blockId uint16) {
	fi.Index = append(fi.Index, specificIndex{
		Pos:   pos,
		Value: value,
		BlockId: blockId,
	})
}
