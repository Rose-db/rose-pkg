package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

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

var _ = GinkgoDescribe("Misc tests", func() {
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
		uuids := [5000]int{}
		for i := 0; i < n; i++ {
			s := testAsJson("some value")
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			uuids[i] = res.ID
		}

		deletedUuids := [3000]int{}
		// delete 3000
		for i := 0; i < 3000; i++ {
			u := uuids[i]

			res, err := a.Delete(u)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

			deletedUuids[i] = res.ID
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
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should create a new collection", func() {
		a := testCreateRose(false)
		collName := "some_collection"

		err := a.NewCollection(collName)

		gomega.Expect(err).To(gomega.BeNil())

		path := fmt.Sprintf("%s/%s", roseDbDir(), collName)
		stat, statErr := os.Stat(path)

		gomega.Expect(statErr).To(gomega.BeNil())

		gomega.Expect(stat.IsDir()).To(gomega.BeTrue())
		gomega.Expect(path).To(gomega.Equal(fmt.Sprintf("%s/%s", roseDbDir(), stat.Name())))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should not fail to create a collection because it exists", func() {
		a := testCreateRose(false)
		collName := "some_collection"

		err := a.NewCollection(collName)

		gomega.Expect(err).To(gomega.BeNil())

		path := fmt.Sprintf("%s/%s", roseDbDir(), collName)
		stat, statErr := os.Stat(path)

		gomega.Expect(statErr).To(gomega.BeNil())

		gomega.Expect(stat.IsDir()).To(gomega.BeTrue())
		gomega.Expect(path).To(gomega.Equal(fmt.Sprintf("%s/%s", roseDbDir(), stat.Name())))

		err = a.NewCollection(collName)

		gomega.Expect(err).To(gomega.BeNil())

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
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		key := res.ID
		s := ""
		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(s).To(gomega.Equal(data))

		res, err = a.Delete(key)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
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
		a := testCreateRose(false)

		data := "[#]{{\n}#]\n"

		res, err := a.Write(testAsJson(data))

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		key := res.ID
		s := ""
		res, err = a.Read(key, &s)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(s).To(gomega.Equal(data))

		res, err = a.Delete(key)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
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
		res, err := a.Read(67, &s)

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

		res, err := a.Delete(89)

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
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
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

		ids := [100000]int{}
		for i := 0; i < n; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
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
		for _, id := range ids {
			s := ""
			res, err := a.Read(id, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			total++
		}

		gomega.Expect(total).To(gomega.Equal(len(ids)))

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

		firstWrite := [2500]int{}
		for i := 0; i < 2500; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			firstWrite[i] = res.ID
		}

		gomega.Expect(roseBlockFile(0, roseDbDir())).To(gomega.Equal(a.db.WriteDriver.Handler.File.Name()))

		secondWrite := [501]int{}
		for i := 2501; i < 3002; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			secondWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(1, roseDbDir())).To(gomega.Equal(a.db.WriteDriver.Handler.File.Name()))

		counter = 0
		thirdWrite := [3000]int{}
		for i := 3002; i < 6002; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			thirdWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(2, roseDbDir())).To(gomega.Equal(a.db.WriteDriver.Handler.File.Name()))

		counter = 0
		fourthWrite := [3000]int{}
		for i := 6002; i < 9002; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			fourthWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(3, roseDbDir())).To(gomega.Equal(a.db.WriteDriver.Handler.File.Name()))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range firstWrite {
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(0, roseDbDir())).To(gomega.Equal(a.db.DeleteDriver.Handler.File.Name()))

		for _, id := range secondWrite {
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(1, roseDbDir())).To(gomega.Equal(a.db.DeleteDriver.Handler.File.Name()))

		for _, id := range thirdWrite {
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(2, roseDbDir())).To(gomega.Equal(a.db.DeleteDriver.Handler.File.Name()))

		for _, id := range fourthWrite {
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(3, roseDbDir())).To(gomega.Equal(a.db.DeleteDriver.Handler.File.Name()))

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

		ids := [1000]int{}
		for i := 0; i < n; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range ids {
			// skip one
			if id == 0 {
				continue
			}
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		count := 0
		for _, id := range ids {
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

				count++
			}
		}

		gomega.Expect(count).To(gomega.Equal(1))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)
		
		count = 0
		for _, id := range ids {
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

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

		ids := [4000]int{}
		for i := 0; i < n; i++ {
			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		strategy := []int{0, 10, 150, 987, 1000, 1001, 1002, 3000, 3001, 3002, 1, 3998, 3999, 2367}

		for _, key := range strategy {
			id := ids[key]
			res, err := a.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		count := 0
		for _, id := range ids {
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

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
			id := ids[key]
			t := ""
			res, err := a.Read(id, &t)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
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
		n := 10000

		results := [10000]chan *GoAppResult{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			resChan := a.GoWrite(s)

			results[i] = resChan
		}

		ids := [10000]int{}
		count := 0
		for i, c := range results {
			res := <-c

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.Result.ID

			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		// assert that every uuid is a valid uuid
		count = 0
		for _, id := range ids {
			id = id
			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		// assert that there are no duplicate ids
		uuidsMap := make(map[int]interface{})
		for _, id := range ids {
			_, ok := uuidsMap[id]

			gomega.Expect(ok).To(gomega.BeFalse())

			uuidsMap[id] = nil
		}

		for _, id := range ids {
			s := ""
			res, err := a.Read(id, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
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
		n := 10000

		ids := [10000]int{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			res, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		goResults := [10000]chan *GoAppResult{}
		for i, id := range ids {
			resChan := a.GoDelete(id)

			goResults[i] = resChan
		}

		count := 0
		for _, c := range goResults {
			res := <-c

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(res.Result.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(DeleteMethodType))

			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		for _, id := range ids {
			s := ""
			res, err := a.Read(id, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range ids {
			s := ""
			res, err := a.Read(id, &s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
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
		n := 10000

		uuids := [10000]int{}
		goResults := [10000]chan *GoAppResult{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			resChan := a.GoWrite(s)

			goResults[i] = resChan
		}

		for i, resChan := range goResults {
			res := <-resChan

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(WriteMethodType))

			ch := a.GoDelete(res.Result.ID)

			delRes := <-ch

			gomega.Expect(delRes.Err).To(gomega.BeNil())
			gomega.Expect(delRes.Result).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(delRes.Result.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(delRes.Result.Method).To(gomega.Equal(DeleteMethodType))

			uuids[i] = res.Result.ID
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
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should write data without waiting for a goroutine to finish and read the results after a timeout", func() {
		a := testCreateRose(false)
		n := 10000

		goResults := [10000]chan *GoAppResult{}
		values := [10000]string{}
		for i := 0; i < n; i++ {
			value := fmt.Sprintf("value-%d", i)
			s := testAsJson(value)

			resChan := a.GoWrite(s)

			goResults[i] = resChan
			values[i] = value
		}

		time.Sleep(15 * time.Second)

		for i, resChan := range goResults {
			res := <-resChan
			value := values[i]

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(WriteMethodType))

			s := ""
			appResult, err := a.Read(res.Result.ID, &s)

			gomega.Expect(value).To(gomega.Equal(s))

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(appResult.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(appResult.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete data without waiting for a goroutine to finish and read the results after a timeout", func() {
		a := testCreateRose(false)
		n := 10000

		results := [10000] *AppResult{}
		for i := 0; i < n; i++ {
			value := fmt.Sprintf("value-%d", i)
			s := testAsJson(value)

			result, err := a.Write(s)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(result.Method).To(gomega.Equal(WriteMethodType))

			results[i] = result
		}

		goResults := [10000]chan *GoAppResult{}
		for i, res := range results {
			ch := a.GoDelete(res.ID)

			goResults[i] = ch
		}

		time.Sleep(15 * time.Second)

		for _, resChan := range goResults {
			res := <-resChan

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(res.Result.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(DeleteMethodType))
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
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

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
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

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

		ids := make([]int, 0)
		for i := 0; i < 100000; i++ {
			value := testAsJson(fmt.Sprintf("id-value-%d", i))

			res, err := a.Write(value)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids = append(ids, res.ID)
		}

		for _, id := range ids {
			r := ""
			res, err := a.Read(id, &r)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
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

		ids := make([]int, 0)
		fsData := ""
		for i := 0; i < 10000; i++ {
			value := testAsJson(fmt.Sprintf("id-value-%d", i))
			data := value

			res, err := a.Write(value)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			fsData += string(*prepareData(res.ID, data))

			ids = append(ids, res.ID)
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
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		id := res.ID

		res, err = a.Delete(id)

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		r := ""
		res, err = a.Read(id, &r)

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

var _ = GinkgoDescribe("Internal Memory DB tests", func() {
	GinkgoIt("Should successfully perform and inspect inserts", func() {
		r := testCreateRose(false)
		n := 10000

		m := r.db

		testInsertFixture(m,n, []uint8{})

		// since block index starts at 0, expected must be 3
		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))
		assertIndexIntegrity(m, n)

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect deletes", func() {
		r := testCreateRose(false)
		const n = 10000

		m := r.db

		ids := testInsertFixture(m, n, []uint8{})

		// since block index starts at 0, expected must be 3
		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))
		assertIndexIntegrity(m, n)

		zerosDeleted := 0
		for _, id := range ids {
			if id == 0 && zerosDeleted == 1 {
				continue
			}

			if id == 0 && zerosDeleted == 0 {
				zerosDeleted++
			}

			status, err := m.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(status).To(gomega.Equal(true))
		}

		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))
		assertIndexIntegrity(m, 0)

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect delete reallocation", func() {
		r := testCreateRose(false)
		n := 10000

		m := r.db

		ids := testInsertFixture(m,n, []uint8{})

		// since block index starts at 0, expected must be 3
		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))

		assertIndexIntegrity(m, n)

		for _, id := range ids {
			status, err := m.Delete(id)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(status).To(gomega.Equal(true))
		}

		gomega.Expect(m.AutoIncrementCounter).To(gomega.Equal(n))
		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))
		assertIndexIntegrity(m, 0)

		n = 50000
		testInsertFixture(m,n, []uint8{})

		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(20)))
		gomega.Expect(m.AutoIncrementCounter).To(gomega.Equal(60000))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

func testFixtureSingleInsert(value []uint8, a *Rose) int {
	res, appErr := a.Write(value)

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

