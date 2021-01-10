package rose

import (
	"github.com/valyala/fastjson"
	"sync"
)

type QueryResult struct {
	ID int
	Data []uint8
}

type balancer struct {
	Count uint16
	Next uint16
	queryQueue *queryQueue
	sync.RWMutex
}

type balancerRequest struct {
	BlockNum uint16
	CollName string
	OperationStages map[int]*operatorStages
	Response chan *queueResponse
}

func newBalancer(currentBlockCount uint16) *balancer {
	b := &balancer{
		Next: 0,
	}

	workerNum := b.calcWorkerNum(currentBlockCount)

	b.Count = workerNum
	b.queryQueue = newQueryQueue(workerNum)

	return b
}

func (b *balancer) reSpawnIfNeeded(blockNum uint16) {
	workerNum := b.calcWorkerNum(blockNum)

	b.Lock()
	if workerNum != 0 {
		b.Count += workerNum

		b.queryQueue.spawn(workerNum)
	}
	b.Unlock()
}

func (b *balancer) calcWorkerNum(blockNum uint16) uint16 {
	next := (blockNum / 100 + 1) * 10

	if next > b.Count {
		return 10
	}

	return 0
}

func (b *balancer) safeIncrement() {
	for {
		b.Next++

		if b.Next == b.Count {
			b.Next = 0
		}

		if b.queryQueue.hasIdx(b.Next) {
			return
		}
	}
}

func (b *balancer) Push(item *balancerRequest) ([]QueryResult, Error) {
	queryResults := make([]QueryResult, 0)
	var err *queryError = nil

	responses := make(chan interface{})

	wg := &sync.WaitGroup{}
	wg.Add(int(item.BlockNum))
	go func(wg *sync.WaitGroup) {
		for res := range responses {
			switch v := res.(type) {
			case *queueResponse:
				queryResults = append(queryResults, QueryResult{
					ID:   v.ID,
					Data: v.Body,
				})
			case *queryError:
				if err == nil {
					err = v
				}
			case bool:
				wg.Done()
			}
		}
	}(wg)

	var i uint16
	for i = 0; i < item.BlockNum; i++ {
		comm := b.queryQueue.Comm[b.Next]

		queueItem := &queueItem{
			CollName: item.CollName,
			OperationStages: item.OperationStages,
			BlockId:  i,
			Check: singleCollectionQueryChecker,
			Response: responses,
		}

		comm<- queueItem

		b.safeIncrement()
	}

	wg.Wait()

	close(responses)

	if err != nil {
		queryResults = make([]QueryResult, 0)
	}

	return queryResults[0:], err
}

func (b *balancer) Close() {
	b.queryQueue.Close()
}

func singleCollectionQueryChecker(v *fastjson.Value, item *queueItem, found *lineReaderData) {
	stages := item.OperationStages

	queueResponse := &queueResponse{
		ID:   found.id,
		Body: found.val,
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

			if v.Exists(cond.field) {
				res := v.GetStringBytes(cond.field)

				if cond.queryType == equality {
					if string(res) == cond.value.(string) {
						if oneOperatorOnly {
							item.Response<- queueResponse

							return
						}

						stageResults++
						success = true
					}
				} else if cond.queryType == inequality {
					if string(res) != cond.value.(string) {
						if oneOperatorOnly {
							item.Response<- queueResponse

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
			item.Response<- queueResponse

			return
		}
	}
}


