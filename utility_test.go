package rose

import (
	"encoding/json"
	"fmt"
	"github.com/onsi/ginkgo"
	"io/ioutil"
	"os"
)

func benchmarkRemoveFileSystemDb() {
	dir := roseDbDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		panic(err)
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
		}
	}
}

func testAsJson(j string) []uint8 {
	js, err := json.Marshal(j)

	if err != nil {
		ginkgo.Fail(fmt.Sprintf("Cannot marshal json with message: %s", err.Error()))
	}

	return js
}