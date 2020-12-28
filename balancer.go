package rose

import (
	"fmt"
	"github.com/valyala/fastjson"
	"os"
	"sync"
)

type QueryResult struct {
	ID int
	Data []uint8
}

type queryQueue struct {
	Comm []chan *queueItem
}

type balancer struct {
	Count uint16
	Next uint16
	queryQueue *queryQueue
	sync.RWMutex
}

type queueResponse struct {
	ID int
	Body []uint8
}

type balancerRequest struct {
	BlockNum uint16
	Item struct {
		CollName string
		Field string
		Value interface{}
		DataType dataType
	}
	Response chan *queueResponse
}

type queueItem struct {
	BlockId uint16
	CollName string
	Field string
	Value interface{}
	dataType dataType
	Response chan interface{}
}

func newQueryQueue(workerNum uint16) *queryQueue {
	qq := &queryQueue{
		Comm: make([]chan *queueItem, 0),
	}

	qq.spawn(workerNum)

	return qq
}

func (qq *queryQueue) Close() {
	for _, c := range qq.Comm {
		if c != nil {
			close(c)
		}
	}
}

func (qq *queryQueue) spawn(workerNum uint16) {
	var i uint16
	for i = 0; i < workerNum; i++ {
		c := make(chan *queueItem)
		qq.Comm = append(qq.Comm, c)

		go qq.runWorker(c)
	}
}

func (qq *queryQueue) runWorker(c chan *queueItem) {
	for item := range c {
		blockPath := roseBlockFile(item.BlockId, fmt.Sprintf("%s/%s", roseDbDir(), item.CollName))

		file, err := createFile(blockPath, os.O_RDONLY)

		if err != nil {
			item.Response<- &queryError{
				Code:    QueryErrorCode,
				Message: fmt.Sprintf("Query resulted in an error: %s", "some error"),
			}

			item.Response<- true

			continue
		}

		reader := NewLineReader(file)
		var p fastjson.Parser

		for {
			_, d, err := reader.Read()

			if err != nil && err.GetCode() == EOFErrorCode {
				break
			}

			if d == nil {
				item.Response<- &queryError{
					Code:    QueryErrorCode,
					Message: "Unable to read a row during query search",
				}

				break
			}

			v, jErr := p.Parse(string(d.val))

			if jErr != nil {
				item.Response<- &queryError{
					Code:    QueryErrorCode,
					Message: fmt.Sprintf("Query resulted in an error: %s", jErr.Error()),
				}

				break
			}

			if v.Exists(item.Field) {
				res := v.GetStringBytes(item.Field)

				if string(res) == item.Value.(string) {
					item.Response<- &queueResponse{
						ID:   d.id,
						Body: d.val,
					}
				}
			}
		}

		if err := closeFile(file); err != nil {
			item.Response<- &queryError{
				Code:    QueryErrorCode,
				Message: fmt.Sprintf("Query resulted in an error: %s", err.Error()),
			}
		}

		reader.Close()

		item.Response<- true
	}
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

func (b *balancer) calcWorkerNum(blockNum uint16) uint16 {
	next := (blockNum / 50 + 1) * 10

	if next > b.Count {
		return 10
	}

	return 0
}

func (b *balancer) Push(item *balancerRequest) ([]*QueryResult, Error) {
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

	var i uint16
	for i = 0; i < item.BlockNum; i++ {
		comm := b.queryQueue.Comm[b.Next]

		queueItem := &queueItem{
			BlockId:  i,
			CollName: item.Item.CollName,
			Field:    item.Item.Field,
			Value:    item.Item.Value,
			dataType: item.Item.DataType,
			Response: responses,
		}

		comm<- queueItem

		b.Next++

		if b.Next == b.Count {
			b.Next = 0
		}
	}

	wg.Wait()

	close(responses)

	if err != nil {
		queryResults = make([]*QueryResult, 0)
	}

	return queryResults, err
}

func (b *balancer) Close() {
	b.queryQueue.Close()
}


