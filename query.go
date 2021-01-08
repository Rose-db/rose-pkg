package rose

import (
	"regexp"
	"strings"
)

type opNode struct {
	cond *singleCondition
	next *opNode
	parent *opNode
	nextOp string
	prevOp string
}

type singleCondition struct {
	collName string
	field string
	value interface{}
	dataType dataType
	queryType queryType
}

type queryBuilder struct {
	built bool
	collName string
	opNode *opNode
}

func NewQueryBuilder() *queryBuilder {
	return &queryBuilder{}
}

func newSingleCondition(qb *queryBuilder, collName string, query string, params map[string]interface{}) *singleCondition {
	field, value, queryType := qb.resolveCondition(query, params)
	dt := qb.resolveDataType(value)

	return &singleCondition{
		collName: collName,
		field:    field,
		value:    value,
		dataType: dt,
		queryType: queryType,
	}
}

func (qb *queryBuilder) If(collName string, query string, params map[string]interface{}) {
	parts := strings.Split(query, " ")

	m := make(map[string]string)
	conds := make([]map[string]string, 0)

	for _, p := range parts {
		if p == "&&" || p == "||" {
			m["op"] = p

			conds = append(conds, m)
			m = make(map[string]string)
		} else {
			m["query"] += p
		}
	}

	conds = append(conds, m)

	root := &opNode{
		cond:   nil,
		next:   nil,
		parent: nil,
		nextOp: "",
	}

	for _, cond := range conds {
		j := newSingleCondition(qb, collName, cond["query"], params)

		if root.cond == nil {
			root.cond = j
			root.nextOp = cond["op"]
		} else {
			root.next = &opNode{
				cond:   j,
				next:   nil,
				parent: nil,
				nextOp: cond["op"],
			}

			root.next.parent = root

			if root.parent != nil {
				root.prevOp = root.parent.nextOp
			}

			root = root.next
		}
	}

	for {
		if root.parent != nil {
			root = root.parent
		} else {
			break
		}
	}

	qb.collName = collName
	qb.opNode = root
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

func (qb *queryBuilder) getQueryType(query string) string {
	if len(regexp.MustCompile("&&|\\|\\|").Split(query, -1)) != 1 {
		return "multiple"
	}

	return "single"
}

