package rose

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type reader struct {
	internalReader *bufio.Reader
	reader io.ReadCloser
	buf []uint8
}

type readerData struct {
	id *[]uint8
	val *[]uint8
}

func NewReader(r *os.File) *reader {
	a := bufio.NewReader(r)
	return &reader{
		reader: r,
		internalReader: a,
		buf: make([]uint8, 1),
	}
}

func (s *reader) Read() (*readerData, bool, RoseError) {
	ok, err := s.populateBuffer()

	if !ok {
		return nil, false, nil
	}

	if err != nil {
		return nil, true, err
	}

	d := s.getData()

	s.buf = make([]uint8, 1)

	return d, true, nil
}

func (s *reader) getData() *readerData {
	a := make([]uint8, 1)
	b := make([]uint8, 1)

	idFull := false
	for i := 0; i < len(s.buf); i++ {
		if s.buf[i] == 91 && s.buf[i+1] == 35 && s.buf[i+2] == 91 {
			i = i+2

			continue
		}

		if s.buf[i] == 93 && s.buf[i+1] == 35 && s.buf[i+2] == 93 {
			if !idFull {
				idFull = true
				i = i+2

				continue
			} else {
				break
			}
		}

		if !idFull {
			a = appendByte(a, s.buf[i])
		} else {
			b = appendByte(b, s.buf[i])
		}
	}

	a = a[1:]
	b = b[1:]

	return &readerData{
		id:  &a,
		val: &b,
	}
}

func (s *reader) populateBuffer() (bool, RoseError) {
	for {
		b, err := s.internalReader.ReadByte()

		if err == io.EOF {
			return false, nil
		}

		if err != nil {
			return false, &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Unable to read filesystem database with message: %s", err.Error()),
			}
		}

		if b == 10 {
			break
		}

		s.buf = appendByte(s.buf, b)
	}

	s.buf = s.buf[1:]

	return true, nil
}
