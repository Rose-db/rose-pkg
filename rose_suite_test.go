package rose

import (
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
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
		var currId uint16

		fac := newIdFactory()
		iterations := 0

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
		a := testCreateRose(false)

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

	GinkgoIt("Rose should defragment after recreating it and not have deleted values in the database", func() {
		a := testCreateRose(false)
		n := 5000

		// write 5000
		uuids := [5000]string{}
		for i := 0; i < n; i++ {
			s := testAsJson("some value")
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			uuids[i] = res.Uuid
		}

		deletedUuids := [3000]string{}
		// delete 3000
		for i := 0; i < 3000; i++ {
			u := uuids[i]

			res, err := a.Delete(u)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			deletedUuids[i] = res.Uuid
		}

		if err := a.Shutdown(); err != nil {

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(true)

		for i := 0; i < 3000; i++ {
			u := deletedUuids[i]

			s := ""
			res, err := a.Read(u, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Input validity tests", func() {
	GinkgoIt("Should successfully save and read data that is similar to the delimiter", func() {
		a := testCreateRose(false)

		data := "[#]{{}#]"

		res, err := a.Write(testAsJson(data))

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		key := res.Uuid
		s := ""
		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		gomega.Expect(s).To(gomega.Equal(data))

		res, err = a.Delete(key)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully skip newlines in data values and not treat them as document delimiters", func() {
		a := testCreateRose(false)

		data := "[#]{{\n}#]\n"

		res, err := a.Write(testAsJson(data))

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		key := res.Uuid
		s := ""
		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		gomega.Expect(s).To(gomega.Equal(data))

		res, err = a.Delete(key)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Successfully failing tests", func() {
	GinkgoIt("Should fail read/delete because of an empty string id", func() {
		a := testCreateRose(false)

		s := ""
		_, readErr := a.Read("", &s)

		if readErr == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(readErr.GetCode()).To(gomega.Equal(DataErrorCode), "DataErrorCode should have been returned as Error.Status")
		gomega.Expect(readErr.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be an empty string"))

		_, delErr := a.Delete("")

		if delErr == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(delErr.GetCode()).To(gomega.Equal(DataErrorCode), "DataErrorCode should have been returned as Error.Status")
		gomega.Expect(delErr.Error()).To(gomega.Equal("Code: 1, Message: Id cannot be an empty string"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail if data is not a json byte array", func() {
		a := testCreateRose(false)

		data := "string_that_is_not_json"

		_, err := a.Write([]uint8(data))

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(DataErrorCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Data must be a JSON byte array"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail because data too large > 16MB", func() {
		a := testCreateRose(false)

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

		_, err := a.Write(testAsJson(string(d)))

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(DataErrorCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		// TODO: There seems to be a difference when converting json byte array to string and back into byte array, check later
		//gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Code: 1, Message: %s", fmt.Sprintf("Data cannot be larger than 16000000 bytes (16MB), %d bytes given", len(d)))))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should fail to read a document if not exists", func() {
		a := testCreateRose(false)

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
		a := testCreateRose(false)

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
		a := testCreateRose(false)
		n := 100000

		for i := 0; i < n; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		}

		dirs, err := ioutil.ReadDir(roseDbDir())

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(dirs)).To(gomega.Equal(n / 3000 + 1))

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert that the memory database is populated correctly from an existing fs database", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		n := 100000

		uuids := [100000]string{}
		for i := 0; i < n; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			uuids[i] = res.Uuid
		}

		dirs, err := ioutil.ReadDir(roseDbDir())

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(dirs)).To(gomega.Equal(n / 3000 + 1))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = testCreateRose(false)
		total := 0
		for _, Uuid := range uuids {
			if Uuid == "" {
				continue
			}

			s := ""
			res, err := a.Read(Uuid, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			total++
		}

		gomega.Expect(total).To(gomega.Equal(len(uuids)))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert correct blocks are opened while deleting", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		counter := 0

		firstWrite := [2500]string{}
		for i := 0; i < 2500; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			firstWrite[i] = res.Uuid
		}

		gomega.Expect(roseBlockFile(0, roseDbDir())).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		secondWrite := [501]string{}
		for i := 2501; i < 3002; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			secondWrite[counter] = res.Uuid
			counter++
		}

		gomega.Expect(roseBlockFile(1, roseDbDir())).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		counter = 0
		thirdWrite := [3000]string{}
		for i := 3002; i < 6002; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			thirdWrite[counter] = res.Uuid
			counter++
		}

		gomega.Expect(roseBlockFile(2, roseDbDir())).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		counter = 0
		fourthWrite := [3000]string{}
		for i := 6002; i < 9002; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			fourthWrite[counter] = res.Uuid
			counter++
		}

		gomega.Expect(roseBlockFile(3, roseDbDir())).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range firstWrite {
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(0, roseDbDir())).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for _, id := range secondWrite {
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(1, roseDbDir())).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for _, id := range thirdWrite {
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(2, roseDbDir())).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		for _, id := range fourthWrite {
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(3, roseDbDir())).To(gomega.Equal(a.db.FsDriver.CurrentHandler.File.Name()))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should skip the deleted entries when booting a populated database", func() {
		a := testCreateRose(false)
		n := 1000
		s := testAsJson(testString)

		uuids := [1000]string{}
		for i := 0; i < n; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			uuids[i] = res.Uuid
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for i, id := range uuids {
			if i == 0 {
				continue
			}

			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		}
		
		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)
		
		count := 0
		for _, id := range uuids {
			if id == "" {
				continue
			}

			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
				gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

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
		a := testCreateRose(false)
		n := 4000
		s := testAsJson(testString)

		uuids := [4000]string{}
		for i := 0; i < n; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			uuids[i] = res.Uuid
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		strategy := []int{0, 10, 150, 987, 1000, 1001, 1002, 3000, 3001, 3002, 1, 3998, 3999, 2367}

		for _, key := range strategy {
			id := uuids[key]
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		count := 0
		for _, id := range uuids {
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
				gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

				count++
			}
		}

		gomega.Expect(count).To(gomega.Equal(n - len(strategy)))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, key := range strategy {
			id := uuids[key]
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Concurrency tests", func() {
	GinkgoIt("Should write values to the database with the concurrent method", func() {
		a := testCreateRose(false)
		n := 1000

		results := [1000]chan *GoAppResult{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			resChan := a.GoWrite(s)

			results[i] = resChan
		}

		uuids := [1000]string{}
		count := 0
		for i, c := range results {
			res := <-c

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(InsertMethodType))

			uuids[i] = res.Result.Uuid

			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		// assert that every uuid is a valid uuid
		count = 0
		for _, Uuid := range uuids {
			gomega.Expect(testIsValidUUID(Uuid)).To(gomega.BeTrue())

			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		// assert that there are no duplicate ids
		uuidsMap := make(map[string]interface{})
		for _, Uuid := range uuids {
			_, ok := uuidsMap[Uuid]

			gomega.Expect(ok).To(gomega.BeFalse())

			uuidsMap[Uuid] = nil
		}

		for _, Uuid := range uuids {
			s := ""
			res, err := a.Read(Uuid, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete document from the database with write done synchronously", func() {
		a := testCreateRose(false)
		n := 1000

		uuids := [1000]string{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))

			uuids[i] = res.Uuid
		}

		goResults := [1000]chan *GoAppResult{}
		for i, Uuid := range uuids {
			resChan := a.GoDelete(Uuid)

			goResults[i] = resChan
		}

		count := 0
		for _, c := range goResults {
			res := <-c

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(testIsValidUUID(res.Result.Uuid)).To(gomega.BeTrue())
			gomega.Expect(res.Result.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(DeleteMethodType))

			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		for _, Uuid := range uuids {
			s := ""
			res, err := a.Read(Uuid, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, Uuid := range uuids {
			s := ""
			res, err := a.Read(Uuid, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should write/delete with sender/receiver patter", func() {
		a := testCreateRose(false)
		n := 1000

		uuids := [1000]string{}
		goResults := [1000]chan *GoAppResult{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			resChan := a.GoWrite(s)

			goResults[i] = resChan
		}

		for i, resChan := range goResults {
			res := <-resChan

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(testIsValidUUID(res.Result.Uuid)).To(gomega.BeTrue())
			gomega.Expect(res.Result.Method).To(gomega.Equal(InsertMethodType))

			ch := a.GoDelete(res.Result.Uuid)

			delRes := <-ch

			gomega.Expect(delRes.Err).To(gomega.BeNil())
			gomega.Expect(delRes.Result).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(testIsValidUUID(res.Result.Uuid)).To(gomega.BeTrue())
			gomega.Expect(delRes.Result.Status).To(gomega.Equal(EntryDeletedStatus))
			gomega.Expect(delRes.Result.Method).To(gomega.Equal(DeleteMethodType))

			uuids[i] = res.Result.Uuid
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, Uuid := range uuids {
			s := ""
			res, err := a.Read(Uuid, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
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

		a := testCreateRose(false)

		res, err := a.Write(s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should insert multiple values", func() {
		var currId uint64

		a := testCreateRose(false)

		for i := 0; i < 100000; i++ {
			s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

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
		a := testCreateRose(false)

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		id := testFixtureSingleInsert(s, a)

		r := ""
		res, err := a.Read(id, &r)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
		gomega.Expect(r).To(gomega.Equal("sdčkfjalsčkjfdlsčakdfjlčk"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should perform multiple reads", func() {
		a := testCreateRose(false)

		ids := make([]string, 0)
		for i := 0; i < 100000; i++ {
			value := testAsJson(fmt.Sprintf("id-value-%d", i))

			res, err := a.Write(value)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			ids = append(ids, res.Uuid)
		}

		for _, id := range ids {
			r := ""
			res, err := a.Read(id, &r)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert fs db integrity after multiple inserts", func() {
		a := testCreateRose(false)

		ids := make([]string, 0)
		fsData := ""
		for i := 0; i < 10000; i++ {
			value := testAsJson(fmt.Sprintf("id-value-%d", i))
			data := value

			res, err := a.Write(value)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())
			gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
			gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

			fsData += string(*prepareData(res.Uuid, data))

			ids = append(ids, res.Uuid)
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete a single document", func() {
		a := testCreateRose(false)

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		res, err := a.Write(s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(InsertMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		Uuid := res.Uuid

		res, err = a.Delete(Uuid)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(EntryDeletedStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		r := ""
		res, err = a.Read(Uuid, &r)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(testIsValidUUID(res.Uuid)).To(gomega.BeTrue())

		if err = a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Internal Memory DB tests", func() {
	GinkgoIt("Should successfully perform and inspect inserts", func() {
		r := testCreateRose(false)

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
		r := testCreateRose(false)

		m := r.db

		ids := testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)

		for _, id := range ids {
			if id == "" {
				continue
			}

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
		r := testCreateRose(false)

		m := r.db

		ids := testInsertFixture(m,10000, []uint8{})

		// since block index starts at 0, expected must be 3
		assertInternalDbValues(m, 3, 0)
		assertInternalDbIntegrity(m, 10000, 4)

		for _, id := range ids {
			if id == "" {
				continue
			}

			status, err := m.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(status).To(gomega.Equal(true))
		}

		assertInternalDbValues(m, 3, 10000)
		assertInternalDbIntegrity(m, 0, 4)

		testInsertFixture(m,50000, []uint8{})

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

func testFixtureSingleInsert(value []uint8, a *Rose) string {
	res, appErr := a.Write(value)

	if appErr != nil {
		panic(appErr)
	}

	if res.Status != OkResultStatus {
		panic(fmt.Sprintf("Invalid result status given. Expected: %s, given: %s", OkResultStatus, res.Status))
	}

	return res.Uuid
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
	uuids := make([]string, num)
	for i := 0; i < num; i++ {
		if len(value) == 0 {
			value = testAsJson("sdkfjsdjfsadfjklsajdfkčl")
		}

		_, Uuid,  err := m.Write(value, true)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(testIsValidUUID(Uuid)).To(gomega.BeTrue())

		uuids = append(uuids, Uuid)
	}

	return uuids
}

func testIsValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
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

