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
	Operator interface{}
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

func (b *balancer) Push(item *balancerRequest, t queryType) ([]*QueryResult, Error) {
	queryResults := make([]*QueryResult, 0)
	var err *queryError = nil

	responses := make(chan interface{})

	wg := &sync.WaitGroup{}
	wg.Add(int(item.BlockNum))
	go func(wg *sync.WaitGroup) {
		for res := range responses {
			switch v := res.(type) {
			case *queueResponse:
				queryResults = append(queryResults, &QueryResult{
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

	if v, ok := item.Operator.(*strictCondition); ok {
		var checker func (v *fastjson.Value, item *queueItem, found *lineReaderData)

		if v.cond == equality {
			checker = queryEqChecker
		} else if v.cond == inequality {
			checker = queryNeqChecker
		}

		var i uint16
		for i = 0; i < item.BlockNum; i++ {
			comm := b.queryQueue.Comm[b.Next]

			queueItem := &queueItem{
				BlockId:  i,
				CollName: v.collName,
				Field:    v.field,
				Value:    v.value,
				dataType: v.dataType,
				EqChecker: checker,
				Response: responses,
			}

			comm<- queueItem

			b.safeIncrement()
		}

		wg.Wait()

		close(responses)
	}

	if err != nil {
		queryResults = make([]*QueryResult, 0)
	}

	return queryResults[0:], err
}

func (b *balancer) Close() {
	b.queryQueue.Close()
}

func queryEqChecker(v *fastjson.Value, item *queueItem, found *lineReaderData) {
	if v.Exists(item.Field) {
		res := v.GetStringBytes(item.Field)

		if string(res) == item.Value.(string) {
			item.Response<- &queueResponse{
				ID:   found.id,
				Body: found.val,
			}
		}
	}
}

func queryNeqChecker(v *fastjson.Value, item *queueItem, found *lineReaderData) {
	if v.Exists(item.Field) {
		res := v.GetStringBytes(item.Field)

		if string(res) != item.Value.(string) {
			item.Response<- &queueResponse{
				ID:   found.id,
				Body: found.val,
			}
		}
	}
}


