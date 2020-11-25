package rose

import (
	"encoding/json"
	"fmt"
	"os"
)

func prepareData(id int, data []uint8) *[]uint8 {
	s := []uint8(fmt.Sprintf("%d%s%s%s", id, delim, string(data), "\n"))

	return &s
}

func isJSON(s []uint8) bool {
	var js json.RawMessage
	return json.Unmarshal(s, &js) == nil
}

func appendByte(slice []uint8, data ...uint8) []uint8 {
	m := len(slice)
	n := m + len(data)
	if n > cap(slice) { // if necessary, reallocate
		// allocate double what's needed, for future growth.
		newSlice := make([]uint8, (n+1)*2)
		copy(newSlice, slice)
		slice = newSlice
	}

	slice = slice[0:n]
	copy(slice[m:n], data)
	return slice
}

func validateData(data []uint8) Error {
	if !isJSON(data) {
		return &dataError{
			Code:    DataErrorCode,
			Message: "Data must be a JSON byte array",
		}
	}

	l := len(data)
	if l > maxValSize {
		return &dataError{
			Code:    DataErrorCode,
			Message: fmt.Sprintf("Data cannot be larger than 16000000 bytes (16MB), %d bytes given", l),
		}
	}

	return nil
}


/**
	Creates batches based on {size}. If len(files) < size,
	it creates a single batch with len(files) in that single batch
 */
func createFileInfoBatch(files []os.FileInfo, size int) map[int][]os.FileInfo {
	m := make(map[int][]os.FileInfo)

	currBatch := 0
	idx := 0
	batch := []os.FileInfo{}
	for i, f := range files {
		if i != 0 && i % size == 0 {
			idx = 0
			m[currBatch] = batch
			currBatch++
			batch = []os.FileInfo{}

			batch = append(batch, f)
		} else {
			batch = append(batch, f)
			idx++
		}
	}

	if len(batch) > 0 {
		m[currBatch] = batch
	}

	return m
}