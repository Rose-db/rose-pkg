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
	Comm [10]chan *queueItem
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
	Processed chan bool
}

func newQueryQueue() *queryQueue {
	qq := &queryQueue{
		Comm: [10]chan *queueItem{},
	}

	for i := 0; i < 10; i++ {
		c := make(chan *queueItem)
		qq.Comm[i] = c

		go qq.spawn(c)
	}

	return qq
}

func (qq *queryQueue) Close() {
	for _, c := range qq.Comm {
		close(c)
	}
}

func (qq *queryQueue) spawn(c chan *queueItem) {
	for item := range c {
		blockPath := roseBlockFile(item.BlockId, fmt.Sprintf("%s/%s", roseDbDir(), item.CollName))

		file, err := createFile(blockPath, os.O_RDONLY)

		if err != nil {
			panic(err)
		}

		reader := NewLineReader(file)
		var p fastjson.Parser

		lines := 0
		for {
			_, d, err := reader.Read()

			if err != nil && err.GetCode() == EOFErrorCode {
				break
			}

			if d == nil {
				panic("invalid row, it must not be nil")
			}

			v, jErr := p.Parse(string(d.val))

			if jErr != nil {
				panic(jErr)
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

			lines++
		}

		if err := closeFile(file); err != nil {
			panic(err)
		}

		reader.Close()

		item.Response<- true
	}
}

type balancer struct {
	Count int
	Next int
	queryQueue *queryQueue
}

func newBalancer() *balancer {
	return &balancer{
		Count: 10,
		Next: 0,
		queryQueue: newQueryQueue(),
	}
}

func (b *balancer) Push(item *balancerRequest) []*QueryResult {
	queryResults := make([]*QueryResult, 0)

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

	return queryResults
}

func (b *balancer) Close() {
	b.queryQueue.Close()
}


