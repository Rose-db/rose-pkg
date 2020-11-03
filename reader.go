package rose

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type lineReader struct {
	internalReader *bufio.Reader
	reader io.ReadCloser
	buf []uint8
}

type lineReaderData struct {
	id []uint8
	val []uint8
}
/**
	This reader is only to be used on populating the database since
	its specially created for that specific reason.

	This struct does not have a Close method since, internally it is
	using bufio.Reader which does not have any Close method. Also,
	the os.File is used after this reader gets its job done.

	This reader is also a one-off reader which means you can use it only
	once to iterate contents of the file. After that, you cannot use it again
	and you have to create a new one.
 */
func NewLineReader(r *os.File) *lineReader {
	a := bufio.NewReader(r)
	return &lineReader{
		reader: r,
		internalReader: a,
		buf: make([]uint8, 1),
	}
}
/**
	Reads a single line in a file. Every call to Read() return a single
	line in a file until io.EOF is reached
 */
func (s *lineReader) Read() (*lineReaderData, bool, RoseError) {
	ok, err := s.populateBuffer()

	if !ok {
		s.internalReader = nil
		s.reader = nil
		s.buf = nil

		return nil, false, nil
	}

	if err != nil {
		return nil, true, err
	}

	d := s.getData()

	s.buf = make([]uint8, 1)

	return d, true, nil
}

func (s *lineReader) getData() *lineReaderData {
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

	return &lineReaderData{
		id:  a,
		val: b,
	}
}

func (s *lineReader) populateBuffer() (bool, RoseError) {
	d := ""
	skip := false
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
			if skip == true {
				skip = false

				continue
			}

			break
		}

		if skip {
			continue
		}

		d += string(b)

		if len(d) == 9 && d == "[{[del]}]" {
			skip = true
		}

		s.buf = appendByte(s.buf, b)
	}

	s.buf = s.buf[1:]

	return true, nil
}
