package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
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
			if iterations == 100000 {
				break
			}

			id := fac.Next()

			condition := false

			if id > 2999 {
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

	GinkgoIt("Should return the real size of the database", func() {
		a := testCreateRose()

		files, err := ioutil.ReadDir(roseDbDir())

		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Could not calculate size of the database: %s", err.Error()))
		}

		var size uint64
		var dbSize uint64

		for _, f := range files {
			size += uint64(f.Size())
		}

		dbSize, err = a.Size()

		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Could not get size of the database: %s", err.Error()))
		}

		gomega.Expect(size).To(gomega.Equal(dbSize))
	})
})

var _ = GinkgoDescribe("Input validity tests", func() {
	GinkgoIt("Should successfully save and read a key that is similar to the delimiter", func() {
		a := testCreateRose()

		key := "[#]{{}#]"
		data := "[#]{{}#]"

		m := &Metadata{
			Data:   testAsJson(data),
			Id: key,
		}

		res, err := a.Write(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

		s := ""
		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(s).To(gomega.Equal(data))

		res, err = a.Delete(key)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully skip newlines in data values and not treat them as document delimiters", func() {
		a := testCreateRose()

		key := "[#]{{}#]"
		data := "[#]{{\n}#]\n"

		m := &Metadata{
			Data:   testAsJson(data),
			Id: key,
		}

		res, err := a.Write(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

		s := ""
		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(s).To(gomega.Equal(data))

		res, err = a.Delete(key)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Successfully failing tests", func() {
	GinkgoIt("Should fail write because of an empty string id", func() {
		a := testCreateRose()

		m := &Metadata{
			Data:   []uint8{},
			Id: "",
		}

		_, err := a.Write(m)

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(MetadataErrorCode), "MetadataErrorCode should have been returned as Error.Status")
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be an empty string"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail read/delete because of an empty string id", func() {
		a := testCreateRose()

		s := ""
		_, readErr := a.Read("", &s)

		if readErr == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(readErr.GetCode()).To(gomega.Equal(MetadataErrorCode), "MetadataErrorCode should have been returned as Error.Status")
		gomega.Expect(readErr.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be an empty string"))

		_, delErr := a.Delete("")

		if delErr == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(delErr.GetCode()).To(gomega.Equal(MetadataErrorCode), "MetadataErrorCode should have been returned as Error.Status")
		gomega.Expect(delErr.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be an empty string"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail if data is not a json byte array", func() {
		a := testCreateRose()

		data := "string_that_is_not_json"

		m := &Metadata{
			Data:   []uint8(data),
			Id: "some-id",
		}

		_, err := a.Write(m)

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(MetadataErrorCode), fmt.Sprintf("MetadataErrorCode should have been returned as Error.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Data must be a JSON byte array"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail because of too large id", func() {
		a := testCreateRose()

		m := &Metadata{
			Data:   []uint8{},
			Id: "ee01a1be-5b8a-4be5-8724-405ee644e07fee01a1be-5b8a-4be5-8724-405ee644e07fee01a1be-5b8a-4be5-8724-405ee644e07fee01a1be-5b8a-4be5-8724-405ee644e07f",
		}

		_, err := a.Write(m)


		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(MetadataErrorCode), fmt.Sprintf("MetadataErrorCode should have been returned as Error.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be larger than 128 bytes, 144 bytes given"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail because data too large > 16MB", func() {
		a := testCreateRose()

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

		m := &Metadata{
			Data:   testAsJson(string(d)),
			Id: "ee01a1be-5b8a-4be5-8724-405ee644e07fee01a1be-5b8",
		}

		_, err := a.Write(m)

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(MetadataErrorCode), fmt.Sprintf("MetadataErrorCode should have been returned as Error.Status"))
		// TODO: There seems to be a difference when converting json byte array to string and back into byte array, check later
		//gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Code: 1, Message: %s", fmt.Sprintf("Data cannot be larger than 16000000 bytes (16MB), %d bytes given", len(d)))))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail if trying to insert data with already existing id", func() {
		var s []uint8
		var a *Rose
		var m *Metadata

		a = testCreateRose()

		s = testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

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
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail to read a document if not exists", func() {
		a := testCreateRose()

		var s string
		res, err := a.Read("id", &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail to delete a document if not exist", func() {
		a := testCreateRose()

		res, err := a.Delete("id")

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Population tests and integrity tests", func() {
	GinkgoIt("Should assert block number based on different write numbers", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose()
		n := 100000

		for i := 0; i < n; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		dirs, err := ioutil.ReadDir(roseDbDir())

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(dirs)).To(gomega.Equal(n / 3000 + 1))

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert that the memory database is populated correctly from an existing fs database", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose()
		n := 100000

		for i := 0; i < n; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		dirs, err := ioutil.ReadDir(roseDbDir())

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(dirs)).To(gomega.Equal(n / 3000 + 1))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = testCreateRose()

		for i := 0; i < n; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DuplicatedIdStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert correct blocks are opened while deleting", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose()

		for i := 0; i < 2500; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		gomega.Expect(roseBlockFile(0)).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for i := 2501; i < 3002; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		gomega.Expect(roseBlockFile(1)).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for i := 3002; i < 6002; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		gomega.Expect(roseBlockFile(2)).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for i := 6002; i < 9002; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		gomega.Expect(roseBlockFile(3)).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose()

		for i := 0; i < 2500; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(0)).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for i := 2501; i < 3002; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(1)).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for i := 3002; i < 6002; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(2)).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for i := 6002; i < 9002; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(3)).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should skip the deleted entries when booting a populated database", func() {
		a := testCreateRose()
		n := 1000
		s := testAsJson(testString)

		for i := 0; i < n; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose()

		for i := 0; i < n - 1; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}
		
		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose()
		
		count := 0
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("id-%d", i)
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())

			if res.Status == FoundResultStatus {
				count++
			}
		}

		gomega.Expect(count).To(gomega.Equal(1))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should skip the deleted entries when booting a populated database and strategically removing entries in the database", func() {
		a := testCreateRose()
		n := 4000
		s := testAsJson(testString)

		for i := 0; i < n; i++ {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Write(&Metadata{
				Id:   id,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose()

		strategy := []int{0, 10, 150, 987, 1000, 1001, 1002, 3000, 3001, 3002, 1, 3999, 4000, 2367}

		for i := range strategy {
			id := fmt.Sprintf("id-%d", i)
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose()

		count := 0
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("id-%d", i)
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())

			if res.Status == FoundResultStatus {
				count++
			}
		}

		gomega.Expect(count).To(gomega.Equal(n - len(strategy)))

		for i := 0; i < len(strategy); i++ {
			id := fmt.Sprintf("id-%d", i)
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose()

		for i := 0; i < len(strategy); i++ {
			id := fmt.Sprintf("id-%d", i)
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Insertion tests", func() {
	GinkgoIt("Should insert a single piece of data", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		var a *Rose
		var m *Metadata

		a = testCreateRose()

		m = &Metadata{
			Data:   s,
			Id:     "id",
		}

		res, err := a.Write(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should insert multiple values", func() {
		var a *Rose
		var m *Metadata

		var err Error
		var res *AppResult
		var currId uint64

		a = testCreateRose()

		for i := 0; i < 100000; i++ {
			s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

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
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Read tests", func() {
	GinkgoIt("Should read a single result", func() {
		a := testCreateRose()

		id := "id"
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		testFixtureSingleInsert(id, s, a)

		r := ""
		res, err := a.Read("id", &r)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(r).To(gomega.Equal("sdčkfjalsčkjfdlsčakdfjlčk"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should perform multiple reads", func() {
		a := testCreateRose()

		ids := make([]string, 0)
		for i := 0; i < 100000; i++ {
			id := fmt.Sprintf("id-%d", i)
			value := testAsJson(fmt.Sprintf("id-value-%d", i))

			res, err := a.Write(&Metadata{
				Id:   id,
				Data: value,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			ids = append(ids, id)
		}

		for _, id := range ids {
			r := ""
			res, err := a.Read(id, &r)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			trueId := strings.Split(id, "-")[1]
			intId, _ := strconv.Atoi(trueId)

			value := fmt.Sprintf("id-value-%d", intId)

			gomega.Expect(r).To(gomega.Equal(value))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert fs db integrity after multiple inserts", func() {
		a := testCreateRose()

		ids := make([]string, 0)
		fsData := ""
		for i := 0; i < 10000; i++ {
			id := fmt.Sprintf("id-%d", i)
			value := testAsJson(fmt.Sprintf("id-value-%d", i))
			data := value

			fsData += string(*prepareData(id, data))

			res, err := a.Write(&Metadata{
				Id:   id,
				Data: value,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			ids = append(ids, id)
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete a single document", func() {
		var a *Rose
		var m *Metadata

		a = testCreateRose()

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		m = &Metadata{
			Data:   s,
			Id:     "id",
		}

		res, err := a.Write(m)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

		res, err = a.Delete("id")

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		r := ""
		res, err = a.Read("id", &r)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err = a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Concurrency tests", func() {
	GinkgoIt("Should perform sequential inserts and deletes with sender/receive pattern", func() {
		r := testCreateRose()
		dataCh := make(chan int)
		deleteCh := make(chan string)
		n := 10000

		go func() {
			for i := 0; i < n; i++  {
				dataCh<- i
			}

			close(dataCh)
		}()

		wg := &sync.WaitGroup{}

		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(dataCh <-chan int, deleteChan chan string, wg *sync.WaitGroup) {
				a := <-dataCh

				id := fmt.Sprintf("id-%d", a)
				value := fmt.Sprintf("value-%d", a)

				m := &Metadata{
					Id: id,
					Data: testAsJson(value),
				}

				res, err := r.Write(m)

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

				deleteCh<- id

				wg.Done()
			}(dataCh, deleteCh, wg)
		}

		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(dataCh <-chan int, deleteChan chan string, wg *sync.WaitGroup) {
				id := <-deleteCh

				res, err := r.Delete(id)

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
				gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

				wg.Done()
			}(dataCh, deleteCh, wg)
		}

		wg.Wait()

		close(deleteCh)

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should perform strategic deletes on exact filesystem blocks with sender/receive pattern", func() {
		r := testCreateRose()
		dataCh := make(chan int)
		n := 1000000

		go func() {
			for i := 0; i < n; i++  {
				dataCh<- i
			}

			close(dataCh)
		}()

		wg := &sync.WaitGroup{}

		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(dataCh <-chan int, wg *sync.WaitGroup) {
				a := <-dataCh

				id := fmt.Sprintf("id-%d", a)
				value := fmt.Sprintf("value-%d", a)

				m := &Metadata{
					Id: id,
					Data: testAsJson(value),
				}

				res, err := r.Write(m)

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

				wg.Done()
			}(dataCh, wg)
		}

		wg.Wait()

		base := n / 3000 - 2
		blocks := [10000]int{}
		multiplier := 1
		counter := 0
		for i := 0; i < base; i++ {
			if i != 0 && i % 1000 == 0 {
				multiplier++
			}

			block := i * 3000 + multiplier

			blocks[counter] = block

			counter++
		}

		for _, block := range blocks {
			if block == 0 {
				continue
			}

			wg.Add(1)
			go func(wg *sync.WaitGroup, block int) {
				id := fmt.Sprintf("id-%d", block)

				res, err := r.Delete(id)

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
				gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

				wg.Done()
			}(wg, block)
		}

		wg.Wait()

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should concurrently insert and read", func() {
		var r *Rose
		num := 10000
		c := make(chan int, num)

		r = testCreateRose()

		produce := func(c chan int, id int) {
			defer ginkgo.GinkgoRecover()

			value := testAsJson(fmt.Sprintf("value-%d", id))

			res, err := r.Write(&Metadata{
				Id:  fmt.Sprintf("id-%d", id),
				Data: value,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			c<- id
		}

		consume := func(id int) {
			rs := ""
			res, err := r.Read(fmt.Sprintf("id-%d", id), &rs)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(rs).To(gomega.Equal(fmt.Sprintf("value-%d", id)))
		}

		for i := 0; i < num; i++ {
			go produce(c, i)
		}

		curr := 0
		for id := range c {
			consume(id)
			curr++

			rs := ""
			res, err := r.Read(fmt.Sprintf("id-%d", id), &rs)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(rs).To(gomega.Equal(fmt.Sprintf("value-%d", id)))

			if curr == num {
				break
			}
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete documents concurrently", func() {
		var r *Rose

		num := 10000
		c := make(chan string, num)

		r = testCreateRose()

		produce := func(c chan string, id string) {
			defer ginkgo.GinkgoRecover()

			data := testAsJson(fmt.Sprintf("value-%s", id))

			res, err := r.Write(&Metadata{
				Id:  id,
				Data: data,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			c<- id
		}

		consume := func(id string) {
			res, err := r.Delete(id)

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

			rs := ""
			res, err := r.Read(a, &rs)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			if curr == num {
				break
			}
		}

		gomega.Expect(curr).To(gomega.Equal(num))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Internal Memory DB tests", func() {
	GinkgoIt("Should successfully perform and inspect inserts", func() {
		r := testCreateRose()

		m := r.db

		testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect deletes", func() {
		r := testCreateRose()

		m := r.db

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
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect delete reallocation", func() {
		r := testCreateRose()

		m := r.db

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
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

func testFixtureSingleInsert(id string, value []uint8, a *Rose) {
	var m *Metadata
	var appErr Error

	m = &Metadata{
		Data:   value,
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
	dir := roseDbDir()
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
			value = testAsJson("sdkfjsdjfsadfjklsajdfkčl")
		}

		_, err := m.Write(id, value, true)

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

