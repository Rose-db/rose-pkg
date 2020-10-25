package rose

import (
	"bufio"
	"io"
	"os"
)

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

		if !idFound {
			if b == 32 {
				idFound = true
			} else {
				idBuff = append(idBuff, b)
			}
		} else {
			if b == 10 {
				a := idBuff[1:len(idBuff)]
				b := valBuff[1:len(valBuff)]
				return &idValue{
					id:  &a,
					val: &b,
				}, true, nil
			}

			valBuff = append(valBuff, b)
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, true, err
		}
	}

	return nil, false, nil
}
