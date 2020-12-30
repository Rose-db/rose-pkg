package rose

import "fmt"

type query struct {
	Collection string
	Field string
	Value interface{}
	DataType dataType
}

type and struct {
	List []*query
}

type or struct {
	List []*query
}

type equal struct {
	query
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

func NewAnd(params ...*query) *and {
	return &and{List: params}
}

func NewQuery(collName string, field string, value interface{}, dataType dataType) *query {
	return &query{
		Collection: collName,
		Field:      field,
		Value:      value,
		DataType:   dataType,
	}
}

func NewEqual(collName string, field string, value interface{}, dataType dataType) *equal {
	return &equal{query{
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
			t := &ifStmt{
				Equal: nil,
				And:   op.(*and),
				Or: nil,
			}

			qb.ifStmt = t
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

func (qb *QueryBuilder) validate() string {
	if qb.ifStmt == nil {
		return "There is no 'If' statement to execute. 'If' statement must exist"
	}

	return ""
}


