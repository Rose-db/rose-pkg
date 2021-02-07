package rose

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
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
	id int
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

	lineReaderData, err := s.getData()

	if err != nil {
		return 0, nil, err
	}

	return off, lineReaderData, nil
}

func (s *lineReader) Close() {
	s.internalReader = nil
	s.buf = nil
	s.off = 0
}

func (s *lineReader) getData() (*lineReaderData, Error) {
	buf := string(s.buf)

	if buf == "" {
		return nil, nil
	}

	split := strings.Split(buf, delim)

	if len(split) != 2 {
		return nil, nil
	}

	a := split[0]
	b := split[1]

	id, err := strconv.Atoi(a)

	if err != nil {
		return nil, newError(SystemMasterErrorCode, DataConversionCode, fmt.Sprintf("Unable to convert string to int32 with message: %s", err.Error()))
	}

	return &lineReaderData{
		id:  id,
		val: []uint8(b),
	}, nil
}

func (s *lineReader) populateBuffer() Error {
	b, err := s.internalReader.ReadBytes('\n')

	if err == io.EOF {
		return newError(FilesystemMasterErrorCode, EOFCode, "End of file")
	}

	if err != nil {
		return newError(FilesystemMasterErrorCode, FsPermissionsCode, fmt.Sprintf("Reading file failed with message: %s", err.Error()))
	}

	s.buf = b[:len(b) - 1]

	return nil
}
