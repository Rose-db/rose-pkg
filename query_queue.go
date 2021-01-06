package rose

import (
	"fmt"
	"github.com/valyala/fastjson"
	"os"
	"strings"
)

type queryQueue struct {
	Comm []chan *queueItem
}

type queueItem struct {
	BlockId uint16
	CollName string
	EqChecker func (v *fastjson.Value, item *queueItem, found *lineReaderData)
	Operator *opNode
	Response chan interface{}
}

type queueResponse struct {
	ID int
	Body []uint8
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

func (qq *queryQueue) len() uint16 {
	var l uint16
	for _, c := range qq.Comm {
		if c != nil {
			l++
		}
	}

	return l
}

func (qq *queryQueue) hasIdx(idx uint16) bool {
	return idx <= qq.len()
}

func (qq *queryQueue) runWorker(c chan *queueItem) {
	for item := range c {
		blockPath := roseBlockFile(item.BlockId, fmt.Sprintf("%s/%s", roseDbDir(), item.CollName))

		file, err := createFile(blockPath, os.O_RDONLY)

		if err != nil && strings.Contains(err.Error(), "too many open") {
			file, err = secureBlockingCreateFile(blockPath, os.O_RDONLY)

			if err != nil {
				item.Response<- &queryError{
					Code:    QueryErrorCode,
					Message: fmt.Sprintf("Query resulted in an error: %s", err.Error()),
				}

				item.Response<- true
			}
		}

		if err != nil {
			item.Response<- &queryError{
				Code:    QueryErrorCode,
				Message: fmt.Sprintf("Query resulted in an error: %s", err.Error()),
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

			item.EqChecker(v, item, d)
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
