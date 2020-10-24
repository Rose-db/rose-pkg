package rose

import (
	"fmt"
	"io"
)

type scanner struct {
	token []uint8
	buffer []uint8
	reader io.Reader
}

func (s *scanner) New(r io.Reader) *scanner {
	return &scanner{
		reader: r,
		token: make([]uint8, 16000000),
		buffer: make([]uint8, 16000000),
	}
}

func (s *scanner) Get() ([]uint8, bool, error) {
	curr := 0
	for {
		buff := make([]uint8, 1)
		n, err := s.reader.Read(buff)

		if err == io.EOF {
			return []uint8{}, false,  nil
		}

		v := buff[curr:n]
		
		fmt.Println(v)

		curr++
	}
}
