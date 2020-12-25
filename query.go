package rose

type OpType string

const And OpType = "and"
const Or OpType = "or"
const Equal OpType = "equal"

type Operator interface {
	Type() OpType
}

type Query struct {
	Collection string
	Field string
	Value string
}

type QueryOperator struct {
	op OpType
	Query
}

func (q *QueryOperator) Type() OpType {
	return And
}

type QueryBuilder struct {
	Collections []string
	QueryOperators []*QueryOperator
}

func (qb *QueryBuilder) Use(coll string) *QueryBuilder {
	qb.Collections = append(qb.Collections, coll)

	return qb
}

func (qb *QueryBuilder) AddAnd(qo *QueryOperator) *QueryBuilder {
	return qb.addOperator(qo, And)
}

func (qb *QueryBuilder) AddOr(qo *QueryOperator) *QueryBuilder {
	return qb.addOperator(qo, Or)
}

func (qb *QueryBuilder) Equal(qo *QueryOperator) *QueryBuilder {
	return qb.addOperator(qo, Equal)
}

func (qb *QueryBuilder) addOperator(qo *QueryOperator, t OpType) *QueryBuilder {
	qo.op = t
	qb.QueryOperators = append(qb.QueryOperators, qo)

	return qb
}


