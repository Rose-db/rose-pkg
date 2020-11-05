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
	GinkgoIt("Should fail because of an empty value id", func() {
		var m *Metadata
		var a *Rose

		a = testCreateRose()

		m = &Metadata{
			Data:   []uint8{},
			Id: "",
		}

		_, err := a.Write(m)

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(MetadataErrorCode), fmt.Sprintf("MetadataErrorCode should have been returned as RoseError.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be an empty string"))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail because of too large id", func() {
		var m *Metadata
		var a *Rose

		a = testCreateRose()

		m = &Metadata{
			Data:   []uint8{},
			Id: "ee01a1be-5b8a-4be5-8724-405ee644e07fee01a1be-5b8a-4be5-8724-405ee644e07fee01a1be-5b8a-4be5-8724-405ee644e07fee01a1be-5b8a-4be5-8724-405ee644e07f",
		}

		_, err := a.Write(m)


		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(MetadataErrorCode), fmt.Sprintf("MetadataErrorCode should have been returned as RoseError.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be larger than 128 bytes, 144 bytes given"))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail because data too large > 16MB", func() {
		var m *Metadata
		var a *Rose

		a = testCreateRose()

		str, fsErr := ioutil.ReadFile("large_value.txt")

		if fsErr != nil {
			panic(fsErr)
		}

		// generates a > 16Mb string
		generateData := func() []uint8 {
			s := string(str)

			for {
				s += s

				if len(s) > maxValSize {
					return []uint8(s)
				}
			}
		}

		d := generateData()

		m = &Metadata{
			Data:   d,
			Id: "ee01a1be-5b8a-4be5-8724-405ee644e07fee01a1be-5b8",
		}

		_, err := a.Write(m)

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(MetadataErrorCode), fmt.Sprintf("MetadataErrorCode should have been returned as RoseError.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Code: 1, Message: %s", fmt.Sprintf("Data cannot be larger than 16000000 bytes (16MB), %d bytes given", len(string(d))))))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})


	GinkgoIt("Should fail if trying to insert data with already existing id", func() {
		var s []uint8
		var a *Rose
		var m *Metadata

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

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail to read a document if not exists", func() {
		var a *Rose
		var m *Metadata

		a = testCreateRose()

		m = &Metadata{
			Id:     "id",
		}

		res, err := a.Read(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail to delete a document if not exist", func() {
		var a *Rose

		a = testCreateRose()

		res, err := a.Delete(&Metadata{
			Id: "id",
		})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Insertion tests", func() {
	GinkgoIt("Should insert a single piece of data", func() {
		var s []uint8
		var a *Rose
		var m *Metadata

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

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should insert multiple values", func() {
		var s []uint8
		var a *Rose
		var m *Metadata

		var err RoseError
		var res *AppResult
		var currId uint64

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

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Read tests", func() {
	GinkgoIt("Should read a single result", func() {
		var a *Rose
		var m *Metadata

		a = testCreateRose()

		id := "id"
		data := "id value"
		testFixtureSingleInsert(id, data, a)

		m = &Metadata{
			Id:     "id",
		}

		res, err := a.Read(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(res.Result).To(gomega.Equal("id value"))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should perform multiple reads", func() {
		var a *Rose

		a = testCreateRose()

		ids := make([]string, 0)
		for i := 0; i < 100000; i++ {
			id := fmt.Sprintf("id-%d", i)
			value := fmt.Sprintf("id-value-%d", i)

			res, err := a.Write(&Metadata{
				Id:   id,
				Data: []uint8(value),
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			ids = append(ids, id)
		}

		for _, id := range ids {
			res, err := a.Read(&Metadata{
				Id:  id,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			trueId := strings.Split(id, "-")[1]
			intId, _ := strconv.Atoi(trueId)

			value := fmt.Sprintf("id-value-%d", intId)

			gomega.Expect(res.Result).To(gomega.Equal(value))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert fs db integrity after multiple inserts", func() {
		var a *Rose

		a = testCreateRose()

		ids := make([]string, 0)
		fsData := ""
		for i := 0; i < 10000; i++ {
			id := fmt.Sprintf("id-%d", i)
			value := fmt.Sprintf("id-value-%d", i)
			data := []uint8(value)

			fsData += string(*prepareData(id, data))

			res, err := a.Write(&Metadata{
				Id:   id,
				Data: []uint8(value),
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			ids = append(ids, id)
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete a single document", func() {
		var a *Rose
		var m *Metadata

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
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err = a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Concurrency tests", func() {
	GinkgoIt("Should concurrently insert and read", func() {
		var r *Rose
		num := 100000
		c := make(chan int, num)

		r = testCreateRose()

		produce := func(c chan int, id int) {
			defer ginkgo.GinkgoRecover()

			res, err := r.Write(&Metadata{
				Id:  fmt.Sprintf("id-%d", id),
				Data: []uint8(fmt.Sprintf("value-%d", id)),
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			c<- id
		}

		consume := func(id int) {
			res, err := r.Read(&Metadata{
				Id:  fmt.Sprintf("id-%d", id),
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(res.Result).To(gomega.Equal(fmt.Sprintf("value-%d", id)))
		}

		for i := 0; i < num; i++ {
			go produce(c, i)
		}

		curr := 0
		for id := range c {
			consume(id)
			curr++

			res, err := r.Read(&Metadata{
				Id:  fmt.Sprintf("id-%d", id),
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Result).To(gomega.Equal(fmt.Sprintf("value-%d", id)))

			if curr == num {
				break
			}
		}

		if err := r.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete documents concurrently", func() {
		var r *Rose

		num := 100000
		c := make(chan string, num)

		r = testCreateRose()

		produce := func(c chan string, id string) {
			defer ginkgo.GinkgoRecover()

			res, err := r.Write(&Metadata{
				Id:  id,
				Data: []uint8(fmt.Sprintf("value-%s", id)),
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			c<- id
		}

		consume := func(id string) {
			res, err := r.Delete(&Metadata{
				Id:  id,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		for i := 0; i < num; i++ {
			id := fmt.Sprintf("id-%d", i)

			go produce(c, id)
		}

		curr := 0
		for a := range c {
			consume(a)
			curr++

			res, err := r.Read(&Metadata{
				Id:  a,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))


			if curr == num {
				break
			}
		}

		if err := r.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Internal Memory DB tests", func() {
	GinkgoIt("Should successfully perform and inspect inserts", func() {
		r := testCreateRose()

		m := r.Db

		testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)

		if err := r.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect deletes", func() {
		r := testCreateRose()

		m := r.Db

		ids := testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)

		for _, id := range ids {
			status, err := m.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(status).To(gomega.Equal(true))
		}

		assertInternalDbValues(m, 3, 10000)
		assertInternalDbIntegrity(m, 0, 4)

		if err := r.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect delete reallocation", func() {
		r := testCreateRose()

		m := r.Db

		ids := testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)

		for _, id := range ids {
			status, err := m.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(status).To(gomega.Equal(true))
		}

		assertInternalDbValues(m, 3, 10000)
		assertInternalDbIntegrity(m, 0, 4)

		ids = testInsertFixture(m,50000, []uint8{})

		assertInternalDbValues(m, 16, 0)
		assertInternalDbIntegrity(m, 50000, 17)

		if err := r.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))
		}

		testRemoveFileSystemDb()
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

func testInsertFixture(m *Db, num int, value []uint8) []string {
	ids := make([]string, 0)
	for i := 0; i < num; i++ {
		id := fmt.Sprintf("id-%d", i)
		ids = append(ids, id)

		if len(value) == 0 {
			value = []uint8("sdkfjsdjfsadfjklsajdfkčl")
		}

		_, err := m.Write(id, value)

		gomega.Expect(err).To(gomega.BeNil())
	}

	return ids
}

func assertInternalDbValues(m *Db, expectedMapIdx uint16, freeListLen int) {
	gomega.Expect(m.CurrMapIdx).To(gomega.Equal(expectedMapIdx))
	gomega.Expect(len(m.FreeIdsList)).To(gomega.Equal(freeListLen))
}

func assertInternalDbIntegrity(m *Db, expectedLen int, expectedCapacity int) {
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

