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
	id []uint8
	val []uint8
}
/**
	This reader is only to be used on populating the database since
	its specially created for that specific reason.

	This struct does not have a Close method since, internally it is
	using bufio.Reader which does not have any Close method. Also,
	the os.File is used after this reader gets its job done
 */
func NewReader(r *os.File) *reader {
	a := bufio.NewReader(r)
	return &reader{
		reader: r,
		internalReader: a,
		buf: make([]uint8, 1),
	}
}
/**
	Reads a single line in a file. Every call to Read() return a single
	line in a file until io.EOF is reached
 */
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
		id:  a,
		val: b,
	}
}

func (s *reader) getId() string {
	a := make([]uint8, 1)

	for i := 0; i < len(s.buf); i++ {
		if s.buf[i] == 91 && s.buf[i+1] == 35 && s.buf[i+2] == 91 {
			i = i+2

			continue
		}

		if s.buf[i] == 93 && s.buf[i+1] == 35 && s.buf[i+2] == 93 {
			break
		}

		a = appendByte(a, s.buf[i])
	}

	a = a[1:]

	return string(a)
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
