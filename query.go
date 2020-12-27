package rose
import "C"
import "fmt"

type Query struct {
	Collection string
	Field string
	Value interface{}
	DataType dataType
}

type And struct {
	List []*Query
}

type Equal struct {
	Query
}

type If struct {
	Equal *Equal
	And *And
}

type QueryBuilder struct {
	Ifs []*If
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		Ifs: make([]*If, 0),
	}
}

func NewEqual(collName string, field string, value interface{}, dataType dataType) *Equal {
	return &Equal{Query{
		Collection: collName,
		Field:      field,
		Value:      value,
		DataType: dataType,
	}}
}

func (qb *QueryBuilder) If(op interface{}) *QueryBuilder {
	switch v := op.(type) {
	case *Equal:
		t := &If{
			Equal: op.(*Equal),
			And:   nil,
		}

		qb.Ifs = append(qb.Ifs, t)
	case *And:
		fmt.Println(v)
	}

	return qb
}

func (qb *QueryBuilder) And(qo []*Query) *And {
	and := &And{
		List:          make([]*Query, 0),
	}

	and.List = append(and.List, qo...)

	return and
}


