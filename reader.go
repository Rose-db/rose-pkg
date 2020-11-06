package rose

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type lineReader struct {
	internalReader *bufio.Reader
	reader io.ReadCloser
	buf []uint8
}

type offsetReader struct {
	internalReader *bufio.Reader
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
	buf := string(s.buf)

	split := strings.Split(buf, delim)

	if len(split) != 2 {
		return nil
	}

	a := split[0]
	b := split[1]

	return &lineReaderData{
		id:  []uint8(a),
		val: []uint8(b),
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

func NewOffsetReader(f *os.File) *offsetReader {
	a := bufio.NewReader(f)
	return &offsetReader{
		internalReader: a,
		buf:            make([]uint8, 1),
	}
}

func (r *offsetReader) GetOffset(id string) (bool, int64, RoseError)  {
	var offset int64 = 0

	for {
		status, err := r.populateBuffer()

		if err != nil {
			return false, 0, err
		}

		if !status {
			return false, 0, nil
		}

		if status {
			buf := string(r.buf)

			s := strings.Split(buf, delim)

			if s[0] == id {
				return true, offset, nil
			} else {
				offset += int64(len(buf))
			}

			r.buf = make([]uint8, 1)
		}
	}
}

func (r *offsetReader) populateBuffer() (bool, RoseError) {
	for {
		b, err := r.internalReader.ReadByte()

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
			r.buf = appendByte(r.buf, b)

			break
		}

		r.buf = appendByte(r.buf, b)
	}

	r.buf = r.buf[1:]

	return true, nil
}
