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
	field, placeholder := qb.resolveCondition(query)
	val := params[placeholder]
	dt := qb.resolveDataType(val)

	eq := &strictEquality{
		collName: collName,
		field:    field,
		value:    val,
		dataType: dt,
	}

	qb.strictEquality = eq
	qb.built = true
}

func (qb *queryBuilder) resolveCondition(query string) (string, string) {
	conds := []string{
		"==",
		"!=",
	}

	for _, c := range conds {
		s := strings.Split(query, c)

		if len(s) != 0 {
			field := strings.TrimSpace(s[0])
			placeholder := strings.TrimSpace(s[1])

			return field, placeholder
		}
	}

	return "", ""
}

func (qb *queryBuilder) resolveDataType(val interface{}) dataType {
	var dt dataType

	switch val.(type) {
	case string:
		dt = stringType
	case int:
		dt = intType
	case float32:
		dt = floatType
	}

	return dt
}

