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
	off int64
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
		internalReader: a,
		buf: make([]uint8, 0),
	}
}
/**
Reads a single line in a file. Every call to Read() return a single
line in a file until io.EOF is reached
*/
func (s *lineReader) Read() (int64, *lineReaderData, Error) {
	for {
		err := s.populateBuffer()

		if err != nil {
			return 0, nil, err
		}

		if string(s.buf[0:9]) == delMark {
			s.off += int64(len(s.buf)) + 1

			continue
		}

		break
	}

	off := s.off
	s.off += int64(len(s.buf)) + 1

	return off, s.getData(), nil
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

func (s *lineReader) populateBuffer() Error {
	b, err := s.internalReader.ReadBytes('\n')

	if err == io.EOF {
		return &endOfFileError{
			Code:    EOFErrorCode,
			Message: "End of file",
		}
	}

	if err != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Reading file failed with message: %s", err.Error()),
		}
	}

	s.buf = b[:len(b) - 1]

	return nil
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

			r.buf = append(r.buf, b)

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

		r.buf = append(r.buf, b)
	}

	r.buf = r.buf[1:]

	return true, nil
}
