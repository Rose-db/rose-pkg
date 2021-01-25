package rose

type opNode struct {
	cond *singleCondition
	next *opNode
	parent *opNode
	nextOp string
	prevOp string
}

type queryBuilder struct {
	built bool
	singleQuery *singleQuery
}

func NewQueryBuilder() *queryBuilder {
	return &queryBuilder{}
}

func (qb *queryBuilder) If(collName string, query string, params map[string]interface{}) {
	qb.singleQuery = newSingleQuery(collName, query, params)
}

