package rose

import (
	"strings"
)

type strictCondition struct {
	collName string
	field string
	value interface{}
	dataType dataType
	cond queryType
}

type queryBuilder struct {
	built bool
	strictCondition *strictCondition
}

func NewQueryBuilder() *queryBuilder {
	return &queryBuilder{}
}

func (qb *queryBuilder) If(collName string, query string, params map[string]interface{}) {
	field, value, cond := qb.resolveCondition(query, params)
	dt := qb.resolveDataType(value)

	eq := &strictCondition{
		collName: collName,
		field:    field,
		value:    value,
		dataType: dt,
		cond: cond,
	}

	qb.strictCondition = eq
	qb.built = true
}

func (qb *queryBuilder) resolveCondition(query string, params map[string]interface{}) (string, interface{}, queryType) {
	conds := []string{
		"==",
		"!=",
	}

	for _, c := range conds {
		s := strings.Split(query, c)

		if len(s) == 2 {
			field := strings.TrimSpace(s[0])
			p := strings.TrimSpace(s[1])
			var value interface{}

			if strings.Contains(p, ":") {
				value = params[p]
			} else {
				value = p
			}

			if c == "==" {
				return field, value, equality
			} else if c == "!=" {
				return field, value, inequality
			}
		}
	}

	return "", "", ""
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

