package rose

import "strings"

type strictEquality struct {
	collName string
	field string
	value interface{}
	dataType dataType
}

type queryBuilder struct {
	built bool
	strictEquality *strictEquality
}

func NewQueryBuilder() *queryBuilder {
	return &queryBuilder{}
}

func (qb *queryBuilder) If(collName string, query string, params map[string]interface{}) {
	s := strings.Split(query, "==")

	field := strings.TrimSpace(s[0])
	placeholder := strings.TrimSpace(s[1])

	val := params[placeholder]
	var dt dataType

	switch val.(type) {
	case string:
		dt = stringType
	case int:
		dt = intType
	case float32:
		dt = floatType
	}

	eq := &strictEquality{
		collName: collName,
		field:    field,
		value:    val,
		dataType: dt,
	}

	qb.strictEquality = eq
	qb.built = true
}