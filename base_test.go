package rose

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var GomegaRegisterFailHandler = gomega.RegisterFailHandler
var GinkgoFail = ginkgo.Fail
var GinkgoRunSpecs = ginkgo.RunSpecs
var GinkgoBeforeSuite = ginkgo.BeforeSuite
var GinkgoAfterSuite = ginkgo.AfterSuite
var GinkgoDescribe = ginkgo.Describe
var GinkgoIt = ginkgo.It

type TestUser struct {
	Type      string  `json:"type"`
	Email     string  `json:"email"`
	IsValid   bool    `json:"isValid"`
	Price     float64 `json:"price"`
	RandomNum int     `json:"randomNum"`
	CreatedAt string  `json:"createdAt"`
	UpdatedAt string  `json:"updatedAt"`
}

type TestProfile struct {
	Name     string `json:"type"`
	Lastname string `json:"lastName"`
	Age      int    `json:"age"`
}

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

func testCreateRose(output bool) *Rose {
	var a *Rose

	a, err := New(output)

	gomega.Expect(err).To(gomega.BeNil())

	return a
}

func testCreateCollection(r *Rose, collName string) string {
	err := r.NewCollection(collName)

	gomega.Expect(err).To(gomega.BeNil())

	return collName
}

func testRemoveFileSystemDb(dir string) {
	if dir == "" {
		panic("Empty dir given to testRemoveFileSystemDb()")
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		panic(err)
	}

	files, err := ioutil.ReadDir(dir)

	gomega.Expect(err).To(gomega.BeNil())

	for _, f := range files {
		if f.IsDir() {
			testRemoveFileSystemDb(fmt.Sprintf("%s/%s", dir, f.Name()))
		}

		if !f.IsDir() {
			err = os.Remove(fmt.Sprintf("%s/%s", dir, f.Name()))

			gomega.Expect(err).To(gomega.BeNil())
		}
	}

	err = os.RemoveAll(dir)

	gomega.Expect(err).To(gomega.BeNil())
}

func testMultipleConcurrentInsert(num int, value string, r *Rose, collName string) map[int]int {
	ids := make(map[int]int, num)

	for i := 0; i < num; i++ {
		if len(value) == 0 {
			value = testAsJson(value)
		}

		res := testSingleConcurrentInsert(WriteMetadata{Data: value, CollectionName: collName}, r)

		ids[i] = res.ID
	}

	return ids
}

func testSingleConcurrentInsert(w WriteMetadata, r *Rose) *AppResult {
	resChan := make(chan *AppResult)
	go func() {
		res, err := r.Write(w)

		gomega.Expect(err).To(gomega.BeNil())

		resChan <- res
	}()

	return <-resChan
}

func testSingleDelete(w DeleteMetadata, r *Rose) *AppResult {
	resChan := make(chan *AppResult)
	go func() {
		res, err := r.Delete(w)

		gomega.Expect(err).To(gomega.BeNil())

		resChan <- res
	}()

	return <-resChan
}

func testSingleReplace(m ReplaceMetadata, r *Rose) *AppResult {
	resChan := make(chan *AppResult)
	go func() {
		res, err := r.Replace(m)

		gomega.Expect(err).To(gomega.BeNil())

		resChan <- res
	}()

	return <-resChan
}

func testSingleRead(w ReadMetadata, r *Rose) *AppResult {
	resChan := make(chan *AppResult)
	go func() {
		res, err := r.Read(w)
		gomega.Expect(err).To(gomega.BeNil())

		resChan <- res
	}()

	return <-resChan
}

func assertIndexIntegrity(m *db, expectedLen int) {
	index := m.PrimaryIndex

	gomega.Expect(len(index)).To(gomega.Equal(expectedLen))
}

func testAsJson(j string) string {
	js, err := json.Marshal(j)

	if err != nil {
		ginkgo.Fail(fmt.Sprintf("Cannot marshal json with message: %s", err.Error()))
	}

	return string(js)
}

func testAsJsonInterface(j interface{}) string {
	js, err := json.Marshal(j)

	if err != nil {
		ginkgo.Fail(fmt.Sprintf("Cannot marshal json with message: %s", err.Error()))
	}

	return string(js)
}
