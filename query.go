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

func (qb *queryBuilder) Do(collName string, query string, tp dataType) {
	s := strings.Split(query, "==")

	field := strings.TrimSpace(s[0])
	value := strings.TrimSpace(s[1])

	eq := &strictEquality{
		collName: collName,
		field:    field,
		value:    value,
		dataType: tp,
	}

	qb.strictEquality = eq
	qb.built = true
}