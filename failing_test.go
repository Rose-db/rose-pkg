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
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		a := testCreateRose(false)

		resChan := make(chan *AppResult)
		go func() {
			res, err := a.Write(WriteMetadata{
				CollectionName: "not_exists",
				Data:           s,
			})

			gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
			gomega.Expect(err.GetMasterCode()).To(gomega.Equal(GenericMasterErrorCode))

			gomega.Expect(err.Error()).To(gomega.Equal("Invalid write request. Collection not_exists does not exist"))

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
			gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
			gomega.Expect(err.GetMasterCode()).To(gomega.Equal(GenericMasterErrorCode))

			gomega.Expect(err.Error()).To(gomega.Equal("Invalid read request. Collection not_exists does not exist"))

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
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")

		data := "string_that_is_not_json"

		_, err := a.Write(WriteMetadata{Data: data, CollectionName: collName})

		if err == nil {
			ginkgo.Fail(fmt.Sprintf("Failed with error: %s", err.Error()))

			return
		}

		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Data must be a JSON byte array"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail replace() if data is not a json byte array", func() {
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")

		data := "string_that_is_not_json"

		_, err := a.Replace(ReplaceMetadata{Data: data, CollectionName: collName, ID: 0})

		if err == nil {
			ginkgo.Fail(fmt.Sprintf("Failed with error: %s", err.Error()))

			return
		}

		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Data must be a JSON byte array"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail because data too large > 5MB", func() {
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

		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
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

	GinkgoIt("Should fail to readBy if field is an empty string", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		_, err := a.ReadBy(ReadByMetadata{
			CollectionName: collName,
			Field:          "",
			Value:          nil,
			DataType:       "",
		})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid readBy method. 'field' is empty. 'field' must be a non empty string"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to readBy if field does not exists as an index", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		_, err := a.ReadBy(ReadByMetadata{
			CollectionName: collName,
			Field:          "non_exists",
			Value:          nil,
			DataType:       "",
		})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Validation error. Invalid readBy method. '%s' does not exist as an index. In using readBy, 'field' must be indexed", "non_exists")))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to readBy if field does not exists as an index", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		_, err := a.ReadBy(ReadByMetadata{
			CollectionName: collName,
			Field:          "non_exists",
			Value:          nil,
			DataType:       "",
		})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Validation error. Invalid readBy method. '%s' does not exist as an index. In using readBy, 'field' must be indexed", "non_exists")))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to readBy if field does not exists as an index", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		err := a.NewIndex(collName, "field", stringIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		_, err = a.ReadBy(ReadByMetadata{
			CollectionName: collName,
			Field:          "field",
			Value:          nil,
			DataType:       "",
		})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid readBy method 'value'. 'value' is empty. 'value' must be a non nil value that corresponds to 'dataType' (int data type -> value must be int)"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to readBy if dataType is an invalid data type", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		err := a.NewIndex(collName, "field", stringIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		_, err = a.ReadBy(ReadByMetadata{
			CollectionName: collName,
			Field:          "field",
			Value:          "some value",
			DataType:       "invalid",
		})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid readBy method 'dataType'. 'dataType' is an invalid data type. Valid data types are int, float, string and bool"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to readBy is provided mismatched data types", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		err := a.NewIndex(collName, "field", stringIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		_, err = a.ReadBy(ReadByMetadata{
			CollectionName: collName,
			Field:          "field",
			Value:          "some value",
			DataType:       "int",
		})

		gomega.Expect(err == (Error)(nil)).To(gomega.Equal(false))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Validation error. Invalid data type. You provided %s but the index is a %s data type", "int", "string")))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

