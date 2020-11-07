package rose

import (
	"encoding/json"
	"fmt"
	"github.com/onsi/ginkgo"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func benchmarkRemoveFileSystemDb() {
	var dir string

	dir = roseDbDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		panic(err)

		return
	}

	files, err := ioutil.ReadDir(dir)

	if err != nil {
		panic(err)

		return
	}

	for _, f := range files {
		err = os.Remove(fmt.Sprintf("%s/%s", dir, f.Name()))

		if err != nil {
			panic(err)

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

func testAsJson(j string) []uint8 {
	js, err := json.Marshal(j)

	if err != nil {
		ginkgo.Fail(fmt.Sprintf("Cannot marshal json with message: %s", err.Error()))
	}

	return js
}