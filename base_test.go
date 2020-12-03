package rose

import (
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

func testInsertFixture(m *Db, num int, value []uint8) map[int]int {
	ids := make(map[int]int, num)
	for i := 0; i < num; i++ {
		if len(value) == 0 {
			value = testAsJson("sdkfjsdjfsadfjklsajdfkčl")
		}

		_, id,  err := m.Write(value, true)

		gomega.Expect(err).To(gomega.BeNil())

		ids[i] = id
	}

	return ids
}

func assertIndexIntegrity(m *Db, expectedLen int) {
	index := m.Index

	gomega.Expect(len(index)).To(gomega.Equal(expectedLen))
}

