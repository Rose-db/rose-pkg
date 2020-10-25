package rose

import (
	"bufio"
	"io"
	"os"
)

type reader struct {
	internalReader *bufio.Reader
	reader io.Reader
}

type idValue struct {
	id *[]uint8
	val *[]uint8
}

func NewReader(r *os.File) *reader {
	a := bufio.NewReader(r)
	return &reader{
		reader: r,
		internalReader: a,
	}
}

func (s *reader) Read() (*idValue, bool, error) {
	idBuff := make([]byte, 1)
	valBuff := make([]byte, 1)
	idFound := false
	for {
		b, err := s.internalReader.ReadByte()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, true, err
		}

		if !idFound {
			if b == 32 {
				idFound = true
			} else {
				idBuff = append(idBuff, b)
			}
		} else {
			if b == 10 {
				a := idBuff[1:]
				b := valBuff[1:]
				return &idValue{
					id:  &a,
					val: &b,
				}, true, nil
			}

			valBuff = append(valBuff, b)
		}
	}

	return nil, false, nil
}
