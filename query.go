package rose

import "fmt"

type query struct {
	Collection string
	Field string
	Value interface{}
	DataType dataType
	AsField string
}

type and struct {
	List []*query
}

type or struct {
	List []*query
}

type ifStmt struct {
	Equal *query
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

func NewQuery(collName string, field string, value interface{}, dataType dataType, asField string) *query {
	return &query{
		Collection: collName,
		Field:      field,
		Value:      value,
		DataType:   dataType,
		AsField: asField,
	}
}

func (qb *QueryBuilder) If(op interface{}) (*QueryBuilder, Error) {
	if qb.initialized {
		return qb, &validationError{
			Code:    ValidationErrorCode,
			Message: "Invalid query. If operator has already been initialized",
		}
	}

	switch v := op.(type) {
		case *query:
			t := &ifStmt{
				Equal: op.(*query),
				And:   nil,
				Or: nil,
			}

			errString := t.validate()

			if errString != "" {
				return nil, &validationError{
					Code:    ValidationErrorCode,
					Message: fmt.Sprintf("Invalid query. %s", errString),
				}
			}

			qb.ifStmt = t
		case *and:
			t := &ifStmt{
				Equal: nil,
				And:   op.(*and),
				Or: nil,
			}

			errString := t.validate()

			if errString != "" {
				return nil, &validationError{
					Code:    ValidationErrorCode,
					Message: fmt.Sprintf("Invalid query. %s", errString),
				}
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

func (i *ifStmt) validate() string {
	if i.Equal != nil {
		if i.Equal.Collection == "" {
			return "Collection name must be a non empty string"
		}

		if i.Equal.Field == "" {
			return "Field name must be a non empty string"
		}

		if i.Equal.Value == nil {
			return "Value name must be a non nil type"
		}

		if !i.Equal.DataType.isValid() {
			return "Invalid data type. Data type can only be 'string', 'int', or 'float'"
		}
	}

	if i.And != nil {
		list := i.And.List

		for _, l := range list {
			if l.Collection == "" {
				return "Collection name must be a non empty string"
			}

			if l.Field == "" {
				return "Field name must be a non empty string"
			}

			if l.Value == nil {
				return "Value name must be a non nil type"
			}

			if !l.DataType.isValid() {
				return "Invalid data type. Data type can only be 'string', 'int', or 'float'"
			}
		}
	}

	return ""
}


