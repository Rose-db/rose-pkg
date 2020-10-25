package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var GomegaRegisterFailHandler = gomega.RegisterFailHandler
var GinkgoFail = ginkgo.Fail
var GinkgoRunSpecs = ginkgo.RunSpecs

func TestRose(t *testing.T) {
	GomegaRegisterFailHandler(GinkgoFail)
	GinkgoRunSpecs(t, "Rose Suite")
}

var GinkgoDescribe = ginkgo.Describe
var GinkgoIt = ginkgo.It

var _ = GinkgoDescribe("Misc tests", func() {
	GinkgoIt("Should generate ids in expected order", func() {
		var fac *idFactory
		var iterations int
		var currId uint16

		fac = newIdFactory()

		for {
			if iterations == 10000000 {
				break
			}

			id := fac.Next()

			condition := false

			if id < 0 || id > 2999 {
				condition = false
			} else {
				condition = true
			}

			gomega.Expect(condition).To(gomega.Equal(true))
			gomega.Expect(currId).To(gomega.Equal(id))

			currId++

			iterations++

			if currId > 2999 {
				currId = 0
			}
		}
	})
})

var _ = GinkgoDescribe("Successfully failing tests", func() {
	GinkgoIt("Should fail because of an invalid id", func() {
		var iv []string
		var m *Metadata
		var a *Rose

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		iv = []string{"insert", "read", "delete"}

		for i := 0; i < len(iv); i++ {
			m = &Metadata{
				Data:   []uint8{},
				Id: "",
			}

			_, err := a.Write(m)

			gomega.Expect(err).NotTo(gomega.BeNil(), err.Error())
			gomega.Expect(err.GetCode()).To(gomega.Equal(MetadataErrorCode), fmt.Sprintf("MetadataErrorCode should have been returned as RoseError.Status"))
			gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be an empty string"))
		}

		a.Shutdown()
	})

	GinkgoIt("Should fail if trying to insert data with already existing id", func() {
		var s []uint8
		var a *Rose
		var m *Metadata

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		s = []uint8("sdčkfjalsčkjfdlsčakdfjlčk")

		m = &Metadata{
			Data:   s,
			Id:     "id",
		}

		res, err := a.Write(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

		res, err = a.Write(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(DuplicatedIdStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

		a.Shutdown()
	})

	GinkgoIt("Should fail to read a document if not exists", func() {
		var a *Rose
		var m *Metadata

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		m = &Metadata{
			Id:     "id",
		}

		res, err := a.Read(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		a.Shutdown()
	})

	GinkgoIt("Should fail to delete a document if not exist", func() {
		var a *Rose

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		res, err := a.Delete(&Metadata{
			Id: "id",
		})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		a.Shutdown()
	})
})

var _ = GinkgoDescribe("Insertion tests", func() {
	GinkgoIt("Should insert a single piece of data", func() {
		var s []uint8
		var a *Rose
		var m *Metadata

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		s = []uint8("sdčkfjalsčkjfdlsčakdfjlčk")

		m = &Metadata{
			Data:   s,
			Id:     "id",
		}

		res, err := a.Write(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

		a.Shutdown()
	})

	GinkgoIt("Should insert multiple values", func() {
		var s []uint8
		var a *Rose
		var m *Metadata

		var err RoseError
		var res *AppResult
		var currId uint64

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		for i := 0; i < 100000; i++ {
			s = []uint8("sdčkfjalsčkjfdlsčakdfjlčk")

			m = &Metadata{
				Data:   s,
				Id:     fmt.Sprintf("id-%d", i),
			}

			res, err = a.Write(m)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			currId++
		}

		a.Shutdown()
	})
})

var _ = GinkgoDescribe("Read tests", func() {
	GinkgoIt("Should read a single result", func() {
		var a *Rose
		var m *Metadata

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		testFixtureSingleInsert("id", "id value", a)

		m = &Metadata{
			Id:     "id",
		}

		res, err := a.Read(m)
		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Result).To(gomega.Equal("id value"))

		a.Shutdown()
	})

	GinkgoIt("Should perform multiple reads", func() {
		var a *Rose

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		ids := make([]string, 0)
		for i := 0; i < 100000; i++ {
			id := fmt.Sprintf("id-%d", i)
			value := fmt.Sprintf("id-value-%d", i)

			_, err := a.Write(&Metadata{
				Id:   id,
				Data: []uint8(value),
			})

			gomega.Expect(err).To(gomega.BeNil())

			ids = append(ids, id)
		}

		for _, id := range ids {
			res, err := a.Read(&Metadata{
				Id:  id,
			})

			gomega.Expect(err).To(gomega.BeNil())

			trueId := strings.Split(id, "-")[1]
			intId, _ := strconv.Atoi(trueId)

			value := fmt.Sprintf("id-value-%d", intId)

			gomega.Expect(res.Result).To(gomega.Equal(value))
		}

		a.Shutdown()
	})

	GinkgoIt("Should delete a single document", func() {
		var a *Rose
		var m *Metadata

		defer testRemoveFileSystemDb()

		a = testCreateRose()

		s := []uint8("sdčkfjalsčkjfdlsčakdfjlčk")

		m = &Metadata{
			Data:   s,
			Id:     "id",
		}

		res, err := a.Write(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

		res, err = a.Delete(&Metadata{
			Id: "id",
		})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		res, err = a.Read(&Metadata{
			Id:  "id",
		})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		a.Shutdown()
	})
})

var _ = GinkgoDescribe("Concurrency tests", func() {
	GinkgoIt("Should concurrently insert and read", func() {
		var r *Rose
		num := 100000
		c := make(chan string, num)

		defer testRemoveFileSystemDb()

		r = testCreateRose()

		produce := func(c chan string, id string) {
			defer ginkgo.GinkgoRecover()

			_, err := r.Write(&Metadata{
				Id:  id,
				Data: []uint8(fmt.Sprintf("value-%s", id)),
			})

			gomega.Expect(err).To(gomega.BeNil())

			c<- id
		}

		consume := func(id string) {
			res, err := r.Read(&Metadata{
				Id:  id,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		}

		for i := 0; i < num; i++ {
			id := fmt.Sprintf("id-%d", i)

			go produce(c, id)
		}

		curr := 0
		for a := range c {
			consume(a)
			curr++

			if curr == num {
				break
			}
		}

		r.Shutdown()
	})

	GinkgoIt("Should delete documents concurrently", func() {
		var r *Rose

		num := 100000
		c := make(chan string, num)

		defer testRemoveFileSystemDb()

		r = testCreateRose()

		produce := func(c chan string, id string) {
			defer ginkgo.GinkgoRecover()

			_, err := r.Write(&Metadata{
				Id:  id,
				Data: []uint8(fmt.Sprintf("value-%s", id)),
			})

			gomega.Expect(err).To(gomega.BeNil())

			c<- id
		}

		consume := func(id string) {
			res, err := r.Delete(&Metadata{
				Id:  id,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
		}

		for i := 0; i < num; i++ {
			id := fmt.Sprintf("id-%d", i)

			go produce(c, id)
		}

		curr := 0
		for a := range c {
			consume(a)
			curr++

			if curr == num {
				break
			}
		}

		r.Shutdown()
	})
})

var _ = GinkgoDescribe("Internal Memory DB tests", func() {
	GinkgoIt("Should successfully perform and inspect inserts", func() {
		m := newMemoryDb()

		testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)
	})

	GinkgoIt("Should successfully perform and inspect deletes", func() {
		m := newMemoryDb()

		ids := testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)

		for _, id := range ids {
			m.Delete(id)
		}

		assertInternalDbValues(m, 3, 10000)
		assertInternalDbIntegrity(m, 0, 4)
	})

	GinkgoIt("Should successfully perform and inspect delete reallocation", func() {
		m := newMemoryDb()

		ids := testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)

		for _, id := range ids {
			m.Delete(id)
		}

		assertInternalDbValues(m, 3, 10000)
		assertInternalDbIntegrity(m, 0, 4)

		ids = testInsertFixture(m,50000, []uint8{})

		assertInternalDbValues(m, 16, 0)
		assertInternalDbIntegrity(m, 50000, 17)
	})
})

var _ = GinkgoDescribe("Internal file handling", func() {
	GinkgoIt("Should scan a file to populate the mem db line by line", func() {
		d := "test_scanner_file.txt"
		maxLines := 100000

		populateTestFile := func(f string, maxLines int) {
			file, err := createFile(f, os.O_RDWR|os.O_CREATE)

			if err != nil {
				panic(err)
			}

			fsDb := newFsDb(file)

			for i := 0; i < maxLines; i++ {
				id := fmt.Sprintf("id-%d", i)
				v := fmt.Sprintf("value-%d", i)
				value := []uint8(v)

				d := prepareData(id, value)

				if err := fsDb.Write(d); err != nil {
					panic(err)
				}
			}

			if err := fsDb.SyncAndClose(); err != nil {
				panic(err)
			}
		}

		populateTestFile(d, maxLines)

		file, err := os.OpenFile(d, os.O_RDONLY, 0666)

		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Test failed with error: %s", err.Error()))
		}

		r := NewReader(file)

		curr := 0
		for {
			val, ok, err := r.Read()

			if err != nil {
				ginkgo.Fail(fmt.Sprintf("Error reading scanner with error: %s", err.Error()))

				return
			}

			if !ok {
				break
			}

			gomega.Expect(string(*val.id)).To(gomega.Equal(fmt.Sprintf("id-%d", curr)))
			gomega.Expect(string(*val.val)).To(gomega.Equal(fmt.Sprintf("value-%d", curr)))

			curr++
		}

		err = os.Remove(d)

		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Failed removing file %s", d))
		}
	})
})

func testFixtureSingleInsert(id string, value string, a *Rose) {
	var s []uint8
	var m *Metadata
	var appErr RoseError
	s = []uint8(value)

	m = &Metadata{
		Data:   s,
		Id:     id,
	}

	_, appErr = a.Write(m)

	if appErr != nil {
		panic(appErr)
	}
}

func testCreateRose() *Rose {
	var a *Rose

	a, err := New(false)

	if err != nil {
		panic(err)
	}

	return a
}

func testRemoveFileSystemDb() {
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

func testInsertFixture(m *memDb, num int, value []uint8) []string {
	ids := make([]string, 0)
	for i := 0; i < num; i++ {
		id := fmt.Sprintf("id-%d", i)
		ids = append(ids, id)

		if len(value) == 0 {
			value = []uint8("sdkfjsdjfsadfjklsajdfkčl")
		}

		m.Write(id, &value)
	}

	return ids
}

func assertInternalDbValues(m *memDb, expectedMapIdx uint16, freeListLen int) {
	gomega.Expect(m.CurrMapIdx).To(gomega.Equal(expectedMapIdx))
	gomega.Expect(len(m.FreeIdsList)).To(gomega.Equal(freeListLen))
}

func assertInternalDbIntegrity(m *memDb, expectedLen int, expectedCapacity int) {
	gomega.Expect(len(m.InternalDb)).To(gomega.Equal(expectedCapacity))

	fullNum := 0

	for _, list := range m.InternalDb {
		for _, a := range list {
			if a != nil {
				fullNum++
			}
		}
	}

	gomega.Expect(fullNum).To(gomega.Equal(expectedLen))
	gomega.Expect(len(m.IdLookupMap)).To(gomega.Equal(expectedLen))
}

