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
				if cond.dataType == stringType {
					convRes := string(c.v.GetStringBytes(cond.field))
					convValue := cond.value.(string)

					if str(convRes).compare(convValue, cond.comparisonType) {
						if oneOperatorOnly {
							c.item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				} else if cond.dataType == boolType {
					convRes := c.v.GetBool(cond.field)
					convValue, _ := strconv.ParseBool(cond.value.(string))

					if boolean(convRes).compare(convValue, cond.comparisonType) {
						if oneOperatorOnly {
							c.item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				} else if cond.dataType == intType {
					convRes := c.v.GetInt(cond.field)
					convValue, _ := strconv.Atoi(cond.value.(string))

					if integer(convRes).compare(convValue, cond.comparisonType) {
						if oneOperatorOnly {
							c.item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				} else if (cond.dataType == floatType) {
					convRes := c.v.GetFloat64(cond.field)
					convValue, _ := strconv.ParseFloat(cond.value.(string), 64)

					if floating(convRes).compare(convValue, cond.comparisonType) {
						if oneOperatorOnly {
							c.item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				}  else if (cond.dataType == dateType) {
					convRes := c.v.GetStringBytes(cond.field)

					dateFieldVal := getDateFromString(string(convRes))
					dateUserVal := getDateFromString(cond.value.(string))

					if dateTime(dateFieldVal).compare(dateUserVal, cond.comparisonType) {
						if oneOperatorOnly {
							c.item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				} else if (cond.dataType == dateTimeType) {
					convRes := c.v.GetStringBytes(cond.field)

					dateFieldVal := getDateFromString(string(convRes))
					dateUserVal := getDateFromString(cond.value.(string))

					if date(dateFieldVal).compare(dateUserVal, cond.comparisonType) {
						if oneOperatorOnly {
							c.item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				} else {
					convRes := string(c.v.GetStringBytes(cond.field))
					convValue := cond.value.(string)

					if str(convRes).compare(convValue, cond.comparisonType) {
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