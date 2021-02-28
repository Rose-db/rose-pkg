package rose

import (
	"fmt"
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

type queryBuilder struct {
	built bool
	query *singleQuery
	conds []string
}

var comparisonOperators = []string{
	"==",
	"!=",
	"<=",
	">=",
	"<",
	">",
}

var conditionalOperators = []string{
	"&&",
	"||",
}

var fieldDataTypes = []dataType {
	intType,
	stringType,
	boolType,
	floatType,
	dateTimeType,
	dateType,
}

func isComparisonOperator(given string) bool {
	for _, op := range comparisonOperators {
		if op == given {
			return true
		}
	}

	return false
}

func isConditionalOperator(given string) bool {
	for _, op := range conditionalOperators {
		if op == given {
			return true
		}
	}

	return false
}

func isCorrectFieldDataType(given string) bool {
	for _, op := range fieldDataTypes {
		if string(op) == given {
			return true
		}
	}

	return false
}

func NewQueryBuilder() *queryBuilder {
	return &queryBuilder{}
}

func (qb *queryBuilder) If(collName string, query string, params map[string]interface{}) Error {
	query, err := validateQuery(query, params)

	if err != nil {
		return err
	}

	qb.query = newSingleQuery(collName, query, params)

	return nil
}

func validateQueryField(f string) Error {
	d := strings.Split(f, ":")

	if len(d) != 2 {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Unable to process query. Invalid field:index given. Field must be in field:index_type format. Index types are %v", fieldDataTypes))
	}

	if !isCorrectFieldDataType(d[1]) {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Unable to process query. Invalid index given. Field must be in field:index_type format. Index formats are %v", fieldDataTypes))
	}

	return nil
}

func validateQuery(query string, params map[string]interface{}) (string, Error) {
	regexp, err := regexp.Compile(`\s+`)

	if err != nil {
		return "", newError(SystemMasterErrorCode, OperatingSystemCode, "Unable to process query. This error should not happen. Please, create a bug report")
	}

	resolved := regexp.ReplaceAllString(query, " ")
	split := strings.Split(resolved, " ")

	grouped := false
	for i := 0; i < len(split); {
		if grouped {
			part := split[i]
			if !isConditionalOperator(part) {
				return "", newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Unable to process query. Invalid conditional operator %s given. Valid conditional operators are %v", part, conditionalOperators))
			}

			grouped = false

			i++
			continue
		}

		a := split[i]
		b := split[i + 1]

		if err := validateQueryField(a); err != nil {
			return "", err
		}

		if !isComparisonOperator(b) {
			return "", newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Unable to process query. Invalid comparison operator given. Comparison operators are %v", comparisonOperators))
		}

		c := split[i + 2]

		if c[0:1] == "#" {
			found := false

			for key, _ := range params {
				if key == c {
					found = true

					break
				}
			}

			if !found {
				return "", newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Unable to process query. Unable to find %s parameter in provided parameters", c))
			}
		}

		i += 3

		grouped = true
	}

	return resolved, nil
}

