package rose

import (
	"sort"
)

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

// Sort sorts index in place, which means that on next usage, it is already sorted based on previous direction (asc, desc)
// Boolean indexes cannot be sorted
func (fi *fieldIndex) Sort(direction sortType) {
	sort.Slice(fi.Index, func(i, j int) bool {
		if direction == sortAsc {
			if fi.DataType == intIndexType {
				return fi.Index[i].Value.(int) < fi.Index[j].Value.(int)
			} else if fi.DataType == stringIndexType {
				return fi.Index[i].Value.(string) < fi.Index[j].Value.(string)
			} else if fi.DataType == floatIndexType {
				return fi.Index[i].Value.(float64) < fi.Index[j].Value.(float64)
			} else if fi.DataType == boolIndexType {
				return fi.Index[i].Value.(float64) < fi.Index[j].Value.(float64)
			}
		}

		if fi.DataType == intIndexType {
			return fi.Index[i].Value.(int) > fi.Index[j].Value.(int)
		} else if fi.DataType == stringIndexType {
			return fi.Index[i].Value.(string) > fi.Index[j].Value.(string)
		} else if fi.DataType == floatIndexType {
			return fi.Index[i].Value.(float64) > fi.Index[j].Value.(float64)
		}

		// defaults to int but it will never come to this
		return fi.Index[i].Value.(int) < fi.Index[j].Value.(int)
	})
}
