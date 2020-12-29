package rose

import "fmt"

type Query struct {
	Collection string
	Field string
	Value interface{}
	DataType dataType
}

type and struct {
	List []*Query
}

type or struct {
	List []*Query
}

type equal struct {
	Query
}

type ifStmt struct {
	Equal *equal
	And *and
	Or *or
}

type QueryBuilder struct {
	ifStmt *ifStmt
	initialized bool
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func NewEqual(collName string, field string, value interface{}, dataType dataType) *equal {
	return &equal{Query{
		Collection: collName,
		Field:      field,
		Value:      value,
		DataType: dataType,
	}}
}

func (qb *QueryBuilder) If(op interface{}) (*QueryBuilder, Error) {
	if qb.initialized {
		return qb, &validationError{
			Code:    ValidationErrorCode,
			Message: "Invalid query. If operator has already been initialized",
		}
	}

	switch v := op.(type) {
		case *equal:
			t := &ifStmt{
				Equal: op.(*equal),
				And:   nil,
				Or: nil,
			}

			qb.ifStmt = t
		case *and:
			fmt.Println(v)
	    case *or:
	    	fmt.Println(v)
		default:
			return qb, &validationError{
				Code:    ValidationErrorCode,
				Message: "Invalid query. Invalid operator given. Expected Equal, And or Or operator",
			}
	}

	qb.initialized = true

	return qb, nil
}

func (qb *QueryBuilder) And(qo []*Query) *and {
	and := &and{
		List:          make([]*Query, 0),
	}

	and.List = append(and.List, qo...)

	return and
}

func (qb *QueryBuilder) validate() string {
	if qb.ifStmt == nil {
		return "There is no 'If' statement to execute. 'If' statement must exist"
	}

	return ""
}


