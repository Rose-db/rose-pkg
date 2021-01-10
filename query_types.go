package rose

import (
	"strings"
)

type singleQuery struct {
	collName string
	opNode *opNode
	stages map[int]*operatorStages
}

func newSingleQuery(collName string, query string, params map[string]interface{}) *singleQuery {
	sq := &singleQuery{
		collName: collName,
	}

	sq.opNode = sq.createCondList(collName, sq.createConditions(query), params)
	sq.stages = sq.createStages(sq.opNode)

	return sq
}

type singleCondition struct {
	collName string
	field string
	value interface{}
	dataType dataType
	comparisonType comparisonType
}

func newSingleCondition(collName string, query string, params map[string]interface{}) *singleCondition {
	sc := &singleCondition{}
	field, value, comparisonType := sc.resolveCondition(query, params)
	field, dt := sc.getExplicitDataType(field)

	sc.collName = collName
	sc.field = field
	sc.value = value
	sc.dataType = dt
	sc.comparisonType = comparisonType

	return sc
}

type operatorStages struct {
	Nodes []*opNode
	Op string
}

func (sc *singleCondition) resolveCondition(query string, params map[string]interface{}) (string, interface{}, comparisonType) {
	conds := []string{
		"==",
		"!=",
		"<",
		">",
		"<=",
		"<=",
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
			} else if c == "<" {
				return field, value, less
			} else if  c == ">" {
				return field, value, more
			} else if c == "<=" {
				return field, value, lessEqual
			} else if c == ">=" {
				return field, value, moreEqual
			}
		}
	}

	return "", "", ""
}

func (sc *singleCondition) resolveDataType(field string, val interface{}) (string, dataType) {
	f, exp := sc.getExplicitDataType(field)

	if exp != "" {
		return f, exp
	}

	return f, ""
}

func (sc *singleCondition) getExplicitDataType(field string) (string, dataType) {
	s := strings.Split(field, ":")

	t := s[1]

	if t == "string" {
		return s[0], stringType
	} else if t == "int" {
		return s[0], intType
	} else if t == "float" {
		return s[0], floatType
	} else if t == "bool" {
		return s[0], boolType
	}

	return field, ""
}


func (sq *singleQuery) createConditions(query string) []map[string]string {
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

	return conds
}

func (sq *singleQuery) createCondList(collName string, conds []map[string]string, params map[string]interface{}) *opNode {
	root := &opNode{
		cond:   nil,
		next:   nil,
		parent: nil,
		nextOp: "",
	}

	for _, cond := range conds {
		j := newSingleCondition(collName, cond["query"], params)

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

	return root
}

func (sq *singleQuery) createStages(root *opNode) map[int]*operatorStages {
	stages := make(map[int]*operatorStages)

	currentStage := 0
	currentOp := root.nextOp

	for {
		if root == nil {
			break
		}

		if stages[currentStage] == nil {
			stages[currentStage] = &operatorStages{
				Nodes: make([]*opNode, 0),
				Op: currentOp,
			}
		}

		stages[currentStage].Nodes = append(stages[currentStage].Nodes, root)

		if currentOp != root.nextOp {
			currentOp = root.nextOp

			currentStage++
		}

		if currentOp == "||" && root.next != nil && root.next.nextOp == "&&" {
			root = root.next
			currentOp = root.nextOp
		} else {
			root = root.next
		}
	}

	return stages
}
