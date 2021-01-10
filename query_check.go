package rose

import (
	"github.com/valyala/fastjson"
	"strconv"
)

type queryCheck struct {
	v *fastjson.Value
	item *queueItem
	found *lineReaderData
}

func (c queryCheck) Check() {
	stages := c.item.OperationStages

	queueResponse := &queueResponse{
		ID:   c.found.id,
		Body: c.found.val,
	}

	oneOperatorOnly := false
	if len(stages) == 1 && len(stages[0].Nodes) == 1 {
		oneOperatorOnly = true
	}

	fullResults := make(map[int]bool)

	for i, stage := range stages {
		stageResults := 0

		if stage.Op == "&&" || stage.Op == "" {
			fullResults[i] = false
		} else if stage.Op == "||" || stage.Op == "" {
			fullResults[i] = false
		}

		for _, node := range stage.Nodes {
			cond := node.cond
			success := false

			if c.v.Exists(cond.field) {
				var convRes interface{}
				var convValue interface{}

				if cond.dataType == stringType {
					convRes = string(c.v.GetStringBytes(cond.field))
					convValue = cond.value.(string)
				} else if cond.dataType == boolType {
					convRes = c.v.GetBool(cond.field)
					convValue, _ = strconv.ParseBool(cond.value.(string))
				} else if cond.dataType == intType {
					convRes = c.v.GetInt(cond.field)
					convValue, _ = strconv.Atoi(cond.value.(string))
				}

				if cond.queryType == equality {
					if convRes == convValue {
						if oneOperatorOnly {
							c.item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				} else if cond.queryType == inequality {
					if convRes != convValue {
						if oneOperatorOnly {
							c.item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				}
			}

			if stage.Op == "&&" && !success {
				break
			}
		}

		if stage.Op == "&&" && stageResults == len(stage.Nodes) {
			fullResults[i] = true
		} else if stage.Op == "||" && stageResults != 0 {
			fullResults[i] = true
		} else {
			fullResults[i] = false
		}
	}

	for _, ok := range fullResults {
		if ok {
			c.item.Response<- queueResponse

			return
		}
	}
}
