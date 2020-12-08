package rose

import (
	"encoding/json"
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"io/ioutil"
	"os"
	"testing"
)

var GomegaRegisterFailHandler = gomega.RegisterFailHandler
var GinkgoFail = ginkgo.Fail
var GinkgoRunSpecs = ginkgo.RunSpecs
var GinkgoBeforeSuite = ginkgo.BeforeSuite
var GinkgoAfterSuite = ginkgo.AfterSuite
var GinkgoDescribe = ginkgo.Describe
var GinkgoIt = ginkgo.It

func TestRose(t *testing.T) {
	GomegaRegisterFailHandler(GinkgoFail)
	GinkgoRunSpecs(t, "Rose Suite")
}

var _ = GinkgoBeforeSuite(func() {
	roseDir := roseDir()

	if err := os.RemoveAll(roseDir); err != nil {
		ginkgo.Fail(fmt.Sprintf("Unable to remove rose dir under %s in BeforeEach", roseDir))
	}
})

var _ = GinkgoAfterSuite(func() {
	roseDir := roseDir()

	if err := os.RemoveAll(roseDir); err != nil {
		ginkgo.Fail(fmt.Sprintf("Unable to remove rose dir under %s in BeforeEach", roseDir))
	}
})

func testFixtureSingleInsert(value []uint8, a *Rose) int {
	res, appErr := a.Write(WriteMetadata{Data: value})

	if appErr != nil {
		panic(appErr)
	}

	if res.Status != OkResultStatus {
		panic(fmt.Sprintf("Invalid result status given. Expected: %s, given: %s", OkResultStatus, res.Status))
	}

	return res.ID
}

func testCreateRose(doDefragmentation bool) *Rose {
	var a *Rose

	a, err := New(doDefragmentation, false)

	if err != nil {
		panic(err)
	}

	return a
}

func testRemoveFileSystemDb() {
	dir := roseDbDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		panic(err)
	}

	files, err := ioutil.ReadDir(dir)

	if err != nil {
		panic(err)
	}

	for _, f := range files {
		err = os.Remove(fmt.Sprintf("%s/%s", dir, f.Name()))

		if err != nil {
			panic(err)
		}
	}
}

func testInsertFixture(m *db, num int, value []uint8) map[int]int {
	ids := make(map[int]int, num)
	for i := 0; i < num; i++ {
		if len(value) == 0 {
			value = testAsJson("sdkfjsdjfsadfjklsajdfkčl")
		}

		_, id,  err := m.Write(value)

		gomega.Expect(err).To(gomega.BeNil())

		ids[i] = id
	}

	return ids
}

func testMultipleConcurrentInsert(num int, value []uint8, r *Rose) map[int]int {
	ids := make(map[int]int, num)

	for i := 0; i < num; i++ {
		if len(value) == 0 {
			value = testAsJson("sdkfjsdjfsadfjklsajdfkčl")
		}

		res := testSingleConcurrentInsert(WriteMetadata{Data: value}, r)

		ids[i] = res.ID
	}

	return ids
}

func testSingleConcurrentInsert(w WriteMetadata, r *Rose) *AppResult {
	resChan := make(chan *AppResult)
	go func() {
		ginkgo.GinkgoRecover()

		res, err := r.Write(w)

		gomega.Expect(err).To(gomega.BeNil())

		resChan<- res
	}()

	return <-resChan
}

func testSingleDelete(w DeleteMetadata, r *Rose) *AppResult {
	resChan := make(chan *AppResult)
	go func() {
		ginkgo.GinkgoRecover()

		res, err := r.Delete(w)

		gomega.Expect(err).To(gomega.BeNil())

		resChan<- res
	}()

	return <-resChan
}

func assertIndexIntegrity(m *db, expectedLen int) {
	index := m.Index

	gomega.Expect(len(index)).To(gomega.Equal(expectedLen))
}

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
