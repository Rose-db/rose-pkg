package rose

import (
	"strings"
)

type singleCondition struct {
	collName string
	field string
	value interface{}
	dataType dataType
	cond queryType
}

type singleCond struct {

}

type multipleCondition struct {
	collName string
}

type queryBuilder struct {
	built bool
	singleCondition *singleCondition
	multipleCondition *multipleCondition
}

func NewQueryBuilder() *queryBuilder {
	return &queryBuilder{}
}

func (qb *queryBuilder) If(collName string, query string, params map[string]interface{}) {
	if qb.getQueryType(query) == "single" {
		field, value, cond := qb.resolveCondition(query, params)
		dt := qb.resolveDataType(value)

		c := &singleCondition{
			collName: collName,
			field:    field,
			value:    value,
			dataType: dt,
			cond: cond,
		}

		qb.singleCondition = c
		qb.built = true
	}
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

func (qb *queryBuilder) getQueryType(query string) string {
	if strings.Contains(query, "&&") || strings.Contains(query, "||") {
		return "multiple"
	}

	return "single"
}

