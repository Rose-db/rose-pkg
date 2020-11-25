package rose

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
)

type lineReader struct {
	internalReader *bufio.Reader
	reader io.ReadCloser
	offset int64
	buf []uint8
}

type offsetReader struct {
	internalReader *bufio.Reader
	buf []uint8
	offset int64
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
func (s *lineReader) Read() (int64, *lineReaderData, bool, Error) {
	currOffset, ok, err := s.populateBuffer()

	if !ok {
		s.internalReader = nil
		s.reader = nil
		s.buf = nil

		return 0, nil, false, nil
	}

	if err != nil {
		return 0, nil, true, err
	}

	d := s.getData()

	offset := s.offset - currOffset

	//s.offset += int64(len(s.buf) + 1)

	s.buf = make([]uint8, 1)

	return offset, d, true, nil
}

func (s *lineReader) getData() *lineReaderData {
	buf := string(s.buf)

	if buf == "" {
		return nil
	}

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

func (s *lineReader) populateBuffer() (int64, bool, Error) {
	d := ""
	skip := false
	var offset int64
	for {
		b, err := s.internalReader.ReadByte()

		if err == io.EOF {
			return 0, false, nil
		}

		if err != nil {
			return 0, false, &dbIntegrityError{
				Code:    DbIntegrityViolationCode,
				Message: fmt.Sprintf("Unable to read filesystem database with message: %s", err.Error()),
			}
		}

		s.offset++
		offset++

		if b == 10 {
			if skip {
				skip = false

				continue
			}

			break
		}

		if skip {
			continue
		}

		d += string(b)

		if len(d) == 9 && d == delMark {
			skip = true
			d = ""
			s.buf = make([]uint8, 1)

			continue
		}

		s.buf = appendByte(s.buf, b)
	}

	s.buf = s.buf[1:]

	return offset, true, nil
}

func NewOffsetReader(f *os.File) *offsetReader {
	a := bufio.NewReader(f)
	return &offsetReader{
		internalReader: a,
		buf:            make([]uint8, 1),
	}
}

func (r *offsetReader) GetOffset(id string) (bool, int64, Error)  {
	for {
		status, err := r.populateBuffer()

		if err != nil {
			return false, 0, err
		}

		if !status {
			return false, 0, nil
		}

		if status {
			s := bytes.Split(r.buf, []uint8(delim))

			if string(s[0]) == id {
				off := r.offset
				r.offset = 0
				off = off - int64(len(r.buf))
				r.buf = make([]uint8, 1)
				return true, off, nil
			}

			r.buf = make([]uint8, 1)
		}
	}
}

func (r *offsetReader) populateBuffer() (bool, Error) {
	skip := false
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

		r.offset++
		if b == 10 {
			if skip {
				skip = false

				continue
			}

			r.buf = appendByte(r.buf, b)

			break
		}

		if skip {
			continue
		}

		if len(r.buf[1:]) == 9 && string(r.buf[1:]) == delMark {
			skip = true
			r.buf = make([]uint8, 1)

			continue
		}

		r.buf = appendByte(r.buf, b)
	}

	r.buf = r.buf[1:]

	return true, nil
}
