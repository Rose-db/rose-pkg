package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"io/ioutil"
	"sync"
)

var _ = GinkgoDescribe("Successfully failing tests", func() {
	GinkgoIt("Should fail to write if the collection does not exist", func() {
		ginkgo.Skip("")

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		a := testCreateRose(false)

		resChan := make(chan *AppResult)
		go func() {
			res, err := a.Write(WriteMetadata{
				CollectionName: "not_exists",
				Data:           s,
			})

			gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(err.GetCode()).To(gomega.Equal(DbIntegrityViolationCode))

			gomega.Expect(err.Error()).To(gomega.Equal("Code: 3, Message: Invalid write request. Collection not_exists does not exist"))

			resChan<- res
		}()

		res := <-resChan

		gomega.Expect(res).To(gomega.BeNil())

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to read if the collection does not exist", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			res, err := a.Read(ReadMetadata{
				CollectionName: "not_exists",
				ID:             0,
				Data:           []uint8("ksdljfčlasjdfklsadfj"),
			})

			gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(res).To(gomega.BeNil())

			gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(err.GetCode()).To(gomega.Equal(DbIntegrityViolationCode))

			gomega.Expect(err.Error()).To(gomega.Equal("Code: 3, Message: Invalid read request. Collection not_exists does not exist"))

			wg.Done()
		}(wg)

		wg.Wait()


		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail write() if data is not a json byte array", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")

		data := "string_that_is_not_json"

		_, err := a.Write(WriteMetadata{Data: []uint8(data), CollectionName: collName})

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(DataErrorCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Data must be a JSON byte array"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail replace() if data is not a json byte array", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")

		data := "string_that_is_not_json"

		_, err := a.Replace(ReplaceMetadata{Data: []uint8(data), CollectionName: collName, ID: 0})

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(DataErrorCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Data must be a JSON byte array"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail because data too large > 16MB", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")

		str, fsErr := ioutil.ReadFile("large_value.txt")

		gomega.Expect(fsErr).To(gomega.BeNil())

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

		_, err := a.Write(WriteMetadata{Data: testAsJson(string(d)), CollectionName: collName})

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(DataErrorCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		// TODO: There seems to be a difference when converting json byte array to string and back into byte array, check later
		//gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Code: 1, Message: %s", fmt.Sprintf("Data cannot be larger than 16000000 bytes (16MB), %d bytes given", len(d)))))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to read a document if not exists", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		var s string
		res := testSingleRead(ReadMetadata{ID: 67, Data: &s, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to delete a document if not exist", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		res, err := a.Delete(DeleteMetadata{ID: 89, CollectionName: collName})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should successfully fail query builder creation", func() {
		a := testCreateRose(false)

		qb := NewQueryBuilder()

		res, err := a.Query(qb)

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(len(res)).To(gomega.Equal(0))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Invalid query. There is no 'If' statement to execute. 'If' statement must exist"))

		qb = NewQueryBuilder()

		qb, err = qb.If(struct {
			Something string
		}{
			Something: "string",
		})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(res).To(gomega.BeNil())
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Invalid query. Invalid operator given. Expected Equal, And or Or operator"))

		qb = NewQueryBuilder()

		qb, err = qb.If(NewQuery("some", "sdklfjs", "", "string", ""))
		qb, err = qb.If(NewQuery("some", "sdklfjs", "", "string", ""))

		gomega.Expect(res).To(gomega.BeNil())
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Invalid query. If operator has already been initialized"))

		qb = NewQueryBuilder()

		qb, err = qb.If(NewQuery("", "field", "", "string", ""))

		gomega.Expect(err == (Error)(nil)).To(gomega.Equal(false))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Invalid query. Collection name must be a non empty string"))

		qb = NewQueryBuilder()

		qb, err = qb.If(NewQuery("coll", "", "", "string", ""))

		gomega.Expect(err == (Error)(nil)).To(gomega.Equal(false))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Invalid query. Field name must be a non empty string"))

		qb = NewQueryBuilder()

		qb, err = qb.If(NewQuery("coll", "sdfsdf", nil, "string", ""))

		gomega.Expect(err == (Error)(nil)).To(gomega.Equal(false))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Invalid query. Value name must be a non nil type"))

		qb = NewQueryBuilder()

		qb, err = qb.If(NewQuery("coll", "sdfsdf", "sdfsdf", "invalid", ""))

		gomega.Expect(err == (Error)(nil)).To(gomega.Equal(false))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Invalid query. Invalid data type. Data type can only be 'string', 'int', or 'float'"))
	})
})

