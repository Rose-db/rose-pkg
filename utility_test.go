package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func benchmarkRemoveFileSystemDb(b *testing.B) {
	var dir string

	dir = roseDbDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		b.Errorf("%s: Database directory .rose_db was not created in %s", dir, testGetBenchmarkName(b))

		return
	}

	files, err := ioutil.ReadDir(dir)

	if err != nil {
		b.Errorf("%s: Removing %s failed with message %s", dir, testGetBenchmarkName(b), err.Error())

		return
	}

	for _, f := range files {
		err = os.Remove(fmt.Sprintf("%s/%s", dir, f.Name()))

		if err != nil {
			b.Errorf("%s: Removing %s failed with message %s", dir, testGetBenchmarkName(b), err.Error())

			return
		}
	}
}

func testGetBenchmarkName(b *testing.B) string {
	v := reflect.ValueOf(*b)
	return v.FieldByName("name").String()
}

func testGetTestName(t *testing.T) string {
	v := reflect.ValueOf(*t)
	return v.FieldByName("name").String()
}